using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp1
{
    [Serializable]
    class DBMS
    {
        public Dictionary<string, Database> Databases { get; set; } = new();
        public string CurrentDataBaseName { get; private set; } = null;

        public bool UseDatabase(string databaseName)
        {
            if (Databases.ContainsKey(databaseName))
            {
                CurrentDataBaseName = databaseName;
                return true;
            }
            return false;
        }
        public void CreateDatabase(string dbName)
        {
            if (!Databases.ContainsKey(dbName))
            {
                Databases[dbName] = new Database(dbName);
                Console.WriteLine($"Database '{dbName}' created.");
            }
            else
            {
                Console.WriteLine("Database already exists.");
            }
        }

        public Database GetCurrentDatabase()
        {
            return CurrentDataBaseName != null ? Databases[CurrentDataBaseName] : null;
        }
    }
}