using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SQLite;
using System.IO;

namespace ConsoleApp19
{

    struct tupel
    {
        string kolom;
        string value;
        public tupel(string k, string v)
        {
            kolom = k;
            value = v;
        }
    }

	class Program
	{
		static SQLiteConnection m_wlConnection;
		static SQLiteConnection m_dbConnection;
		static void Main(string[] args)
		{
		
			createDatabase();
			rundbCommands();
			workloadLoad();

			string command = "Select * from autompg";
			SQLiteCommand henk = new SQLiteCommand(command, m_dbConnection);
			SQLiteDataReader reader = henk.ExecuteReader();
			while (reader.Read())
				Console.WriteLine("id: " + reader["id"] + " name: " + reader["model"]);
			Console.ReadKey();
		}

		public static void createDatabase()
		{
			SQLiteConnection.CreateFile("MyDatabase.sqlite");
			m_dbConnection = new SQLiteConnection("Data Source=MyDatabase.sqlite;Version=3;");
			m_dbConnection.Open();
		}

		public static void rundbCommands()
		{
			try
			{
				using (StreamReader sr = new StreamReader("../../database.txt"))
				{
					string line;

					while ((line = sr.ReadLine()) != null)
					{
						SQLiteCommand com = new SQLiteCommand(line, m_dbConnection);
						com.ExecuteNonQuery();
					}
				}
			}
			catch (Exception e)
			{
				Console.WriteLine("The file could not be read:");
				Console.WriteLine(e.Message);
			}
		}

		public static void qf(string[] q) {

			int idx = 0;
			int times = int.Parse(q[0]);
			for (int i = 0; i < q.Length; i++)
			{
				if (q[i].ToUpper() == "WHERE")
				{
					idx = ++i;
					break;
				}
			}

			string k = q[idx++];
			if (q[idx++] == "=")
			{
				string v = q[idx++];
			}
			else
			{
				string v = q[idx++];
			}

			if (idx < q.Length)
			{
				
			}

		}

		static void workloadLoad()
		{
			try
			{
				using (StreamReader sr = new StreamReader("../../workload.txt"))
				{
					string line;

					while ((line = sr.ReadLine()) != null)
					{
						string[] q = line.Split(' ');
						qf(q);
					}
				}
			}
			catch (Exception e)
			{
				Console.WriteLine("The file could not be read:");
				Console.WriteLine(e.Message);
			}
		}
	}
}
