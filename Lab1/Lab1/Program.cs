using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Lab1
{
    class Program
    {
        static void Main(string[] args)
        {
            string pathApp = GetPath();

            Menu(pathApp);
        }

        /// <summary>
        /// Выбрать путь для хранения файлов программы
        /// </summary>        
        static string GetPath()
        {
            string pathApp = null;

            while (pathApp == null)
            {
                Console.Write("Укажите путь для хранения файлов: ");
                pathApp = Console.ReadLine();
                DirectoryInfo di = new DirectoryInfo(pathApp);

                if (!di.Exists)
                {
                    try
                    {
                        di.Create();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.Message);
                        pathApp = null;
                    }
                }
            } 

            return pathApp;
        }

        static void Menu(string pathApp)
        {
            bool work = true;

            while (work)
            {
                Console.WriteLine();
                Console.WriteLine("1. Сгенерировать файл");
                Console.WriteLine("2. Посчитать и отсортировать");
                Console.WriteLine("3. Выйти");
                Console.Write("Введите номер команды: ");

                string command = Console.ReadLine();
                switch (command)
                {
                    case "1":
                        GenerateBigData(pathApp);
                        break;

                    case "2":
                        Sort(pathApp);
                        break;

                    case "3":
                        work = false;
                        break;

                    default: break;
                }
            }
        }

        /// <summary>
        /// Сгенерировать файл
        /// </summary>
        /// <param name="pathApp">Путь куда пишем</param>        
        static void GenerateBigData(string pathApp)
        {
            double sizeFileGb = 0;

            do
            {
                Console.Write("Укажите размер файла в Гб: ");
            } while (!Double.TryParse(Console.ReadLine(), out sizeFileGb) || sizeFileGb <= 0);

            ulong sizeFileBytes = (ulong)(sizeFileGb * 1024 * 1024 * 1024);

            try
            {
                using (GeneratorOfWords gow = new GeneratorOfWords(pathApp + "/bigdata.txt", sizeFileBytes))
                {
                    gow.Generate();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        /// <summary>
        /// Посчитать и отсортировать
        /// </summary>
        /// <param name="pathApp"></param>
        static void Sort(string pathApp)
        {
            FileInfo fi = new FileInfo(pathApp + "/bigdata.txt");

            if (fi.Exists)
            {
                // делаем ограничение по памяти, когда достигнем, то начинаем скидывать файлы
                // Reducer'ов на жесткий
                double memorySizeGb = 0;

                do
                {
                    Console.Write("Сколько оперативки не жалко (Гб) ?: ");
                } while (!Double.TryParse(Console.ReadLine(), out memorySizeGb) || memorySizeGb <= 0);

                ReducerManager reducersManager = new ReducerManager(memorySizeGb);
                List<Task> tasks = new List<Task>();

                Console.WriteLine(DateTime.Now);

                // Распределяем на 4 (хотя смотря сколько потоков есть) Mapper
                for (int i = 0; i < 4; i++)
                {
                    int ind = i;
                    Task task = Task.Run(() =>
                    {
                        using (StreamReader reader = new StreamReader(pathApp + "/bigdata.txt", Encoding.UTF8))
                        {
                            long sizeBlock = reader.BaseStream.Length / 4;

                            reader.BaseStream.Seek(3 + sizeBlock * ind, SeekOrigin.Begin);

                            if (ind != 0)
                                while ((char)reader.Read() != '\n') ;

                            Mapper m1 = new Mapper(reader, reducersManager, sizeBlock);
                            m1.Process();
                        }
                    });

                    tasks.Add(task);
                }

                Task.WaitAll(tasks.ToArray());
                reducersManager.WriteResult();
                Console.WriteLine(DateTime.Now);
            }
            else
            {
                Console.WriteLine("Файл не сгенерирован!");
            }
        }
    }

    /// <summary>
    /// Класс Рандома, чтобы разные потоки не генерировали одни и те же значения
    /// </summary>
    public static class StaticRandom
    {
        static int seed = Environment.TickCount;

        static readonly ThreadLocal<Random> random =
            new ThreadLocal<Random>(() => new Random(Interlocked.Increment(ref seed)));

        public static int Rand(int min, int max)
        {
            return random.Value.Next(min, max);
        }
    }

    /// <summary>
    /// Класс для генерации файла
    /// </summary>
    class GeneratorOfWords : IDisposable
    {
        private StreamWriter writer;

        private ulong sizeFileBytes = 0;
        private ulong needSizeFileBytes;
        private object locker = new object();

        public GeneratorOfWords(string path, ulong needSizeFileBytes)
        {
            writer = new StreamWriter(path, false, Encoding.UTF8, 512 * 1024 * 1024);
            writer.NewLine = "\n";
            this.needSizeFileBytes = needSizeFileBytes;
        }

        public void Generate()
        {
            Console.WriteLine(DateTime.Now);
            Console.WriteLine("Начинаем запись...");

            Task[] tasks = new Task[4];
            for (int i = 0; i < 4; i++)
            {
                tasks[i] = Task.Run(() =>
                {
                    while (sizeFileBytes < needSizeFileBytes)
                    {
                        string word = GetWord();
                        WriteWordInFile(word);
                    }
                });
            }          

            Task.WaitAll(tasks);
            Console.WriteLine("\nВаши BigData готовы!");
            Console.WriteLine(DateTime.Now);
        }

        private void WriteWordInFile(string word)
        {
            lock (locker)
            {
                sizeFileBytes += (ulong)(word.Length + 1); // +1 из-за символа '\n'
                writer.WriteLine(word);
            }
        }

        private string GetWord()
        {
            int lenWord = StaticRandom.Rand(100, 255);
            string result = "";

            char[] symbols =
            {
                'A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z',
                'a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z',
                '0','1','2','3','4','5','6','7','8','9'
            };

            for (int i = 0; i < lenWord; i++)
            {
                // 26 букв английского (26 * 2 т.к. строчные и прописные) + 10 цифр
                int numSymb = StaticRandom.Rand(0, 61);
                result += symbols[numSymb];
            }

            return result;
        }

        public void Dispose()
        {
            writer.Close();
            writer.Dispose();
        }
    }

    class Mapper
    {
        private ReducerManager reducerManager;
        private StreamReader reader;
        private long sizeBlock;

        public Mapper(StreamReader reader, ReducerManager reducerManager, long sizeBlock)
        {
            this.reader = reader;
            this.reducerManager = reducerManager;
            this.sizeBlock = sizeBlock;
        }

        public void Process()
        {
            // берем по слову, упаковываем в вид, слово - количество повторений
            string line;
            long len = 0;
            while ((line = reader.ReadLine()) != null && len < sizeBlock)
            {
                len += line.Length + 1;

                KeyValuePair<string, byte> t = new KeyValuePair<string, byte>(line, 1);
                Reduce(t);
            }
        }

        private void Reduce(KeyValuePair<string, byte> t)
        {
            // взял Reducer - верни
            Reducer reducer = reducerManager.FindReducer(t.Key);
            reducer.Reduce(t);
            reducerManager.ReturnReducer(reducer);
        }
    }

    /// <summary>
    /// Управляет Reducer'ами, находит нужный
    /// </summary>
    class ReducerManager
    {
        // Список Reducer'ов, ключ - первое слово, тем самым мы можем найти подходящий Reducer
        // и сортируем сразу
        private SortedDictionary<string, Reducer> reducers = new SortedDictionary<string, Reducer>();

        private long memorySizeBytes;
        private object locker = new object();
        private Process process;

        public ReducerManager(double memorySizeGb)
        {
            memorySizeBytes = (long)(memorySizeGb * 1024 *1024 * 1024);
            reducers.Add("", new Reducer(Guid.NewGuid()));
            process = Process.GetCurrentProcess();
        }

        public Reducer FindReducer(string word)
        {
            // ищем подходящий Reducer
            Monitor.Enter(locker);

            Reducer reducer = reducers.First().Value;

            foreach (KeyValuePair<string, Reducer> r in reducers)
            {
                if (String.Compare(word, r.Key) < 0)
                    break;

                reducer = r.Value;
            }

            Monitor.Exit(locker);

            Monitor.Enter(reducer.locker);
            
            // если превышено количество слов в Reducer, то делим его пополам
            if (reducer.Load() > 5000)
            {
                Monitor.Enter(locker);

                Reducer newReducer = reducer.Devide();
                reducers.Add(newReducer.FirstWord, newReducer);

                Monitor.Exit(locker);

                // определяем нужный файл из 2-х
                if (String.Compare(word, newReducer.FirstWord) >= 0)
                {
                    Monitor.Exit(reducer.locker);
                    reducer = newReducer;
                    Monitor.Enter(reducer.locker);
                }
            }

            return reducer;
        }

        public void ReturnReducer(Reducer reducer)
        {   
            // если первое слово изменилось, то меняем и ключ
            if (reducer.OldWord != reducer.FirstWord)
            {
                Monitor.Enter(locker);

                reducers.Remove(reducer.OldWord);
                reducers.Add(reducer.FirstWord, reducer);

                Monitor.Exit(locker);
            }

            // если превысили квоту по памяти, то сбрасываем на диск
            if (GC.GetTotalMemory(false) > memorySizeBytes)
            {                    
                reducer.ClearBuf();
            }

            Monitor.Exit(reducer.locker);
        }

        /// <summary>
        /// Записываем результат в выходной файл
        /// В результате получаем отсортированные слова с кол-вом повторений
        /// </summary>
        public void WriteResult()
        {
            using (StreamWriter sw = new StreamWriter("D:/Lab1/result.txt", false, Encoding.UTF8))
            {
                foreach (var reducer in reducers)
                {
                    SortedList<string, byte> rec = reducer.Value.GetRecords();

                    foreach (var r in rec)
                        sw.WriteLine(r.Key + ", " + r.Value);

                    reducer.Value.Cl2();
                }
            }
        }
    }

    class Reducer
    {
        private string path;
        public object locker = new object();
        private SortedList<string, byte> buf = new SortedList<string, byte>();
        public bool Loaded { get; private set; }
        public string FirstWord { get; private set; }
        public string OldWord { get; private set; }

        public Reducer(Guid id)
        {
            path = "D:/Lab1/" + id + ".dat";
            Loaded = true;

            FirstWord = "";
            OldWord = "";
        }

        public Reducer(Guid id, SortedList<string, byte> records)
        {
            path = "D:/Lab1/" + id + ".dat";
            buf = records;
            OldWord = FirstWord;
            FirstWord = buf.First().Key;
            Loaded = true;
        }

        // считаем слова
        public void Reduce(KeyValuePair<string, byte> tupple)
        {
            if (buf.ContainsKey(tupple.Key))
                buf[tupple.Key]++;
            else
                buf.Add(tupple.Key, tupple.Value);

            OldWord = FirstWord;
            FirstWord = buf.First().Key;
        }

        public void ClearBuf()
        {
            WriteFile();
            buf.Clear();
            Loaded = false;
        }

        // загрузка
        public int Load()
        {
            if (!Loaded)
            {
                BinaryFormatter formatter = new BinaryFormatter();
                using (FileStream fs = new FileStream(path, FileMode.OpenOrCreate))
                {
                    if (fs.Length > 0)
                        buf = (SortedList<string, byte>)formatter.Deserialize(fs);
                }

                Loaded = true;
            }

            FirstWord = buf.Any() ? buf.First().Key : "";
            OldWord = FirstWord;

            return buf.Count;
        }

        /// <summary>
        /// Разделить Reducer на 2 части
        /// </summary>
        /// <returns></returns>
        public Reducer Devide()
        {
            SortedList<string, byte> first = new SortedList<string, byte>();
            SortedList<string, byte> second = new SortedList<string, byte>();

            for (int i = 0; i < buf.Count; i++)
            {
                KeyValuePair<string, byte> el = buf.ElementAt(i);

                if (i < buf.Count / 2)
                    first.Add(el.Key, el.Value);
                else
                    second.Add(el.Key, el.Value);
            }

            Guid idSecond = Guid.NewGuid();
            string pathSecond = "D:/Lab1/" + idSecond + ".dat";
            Reducer secondReducer = new Reducer(idSecond, second);

            buf = first;

            return secondReducer;
        }

        public void Cl2()
        {
            buf.Clear();
        }

        public SortedList<string, byte> GetRecords()
        {
            Load();
            return buf;
        }

        private void WriteFile()
        {
            // создаем объект BinaryFormatter
            BinaryFormatter formatter = new BinaryFormatter();
            // получаем поток, куда будем записывать сериализованный объект
            using (FileStream fs = new FileStream(path, FileMode.Create))
            {
                formatter.Serialize(fs, buf);
            }
        }
    }
}
