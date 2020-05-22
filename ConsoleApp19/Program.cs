using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SQLite;
using System.IO;

namespace ConsoleApp19
{

	public struct tuple
	{
		public string column;
		public string value;
		public tuple(string k, string v)
		{
			column = k;
			value = v;
		}
	}

	class Program
	{
		static Dictionary<tuple, int> freqDict = new Dictionary<tuple, int>();

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

			foreach (KeyValuePair<tuple, int> t in freqDict)
			{
				Console.WriteLine(t.Key.column + " " + t.Key.value + " " + t.Value);
			}
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

		public static void qf(string[] q)
		{

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

			while (idx < q.Length)
			{
				string k = q[idx++];
				if (q[idx++] == "=")
				{
					string v = q[idx++];
					freqCounter(v, k, times);

				}
				else
				{
					char[] temp = q[idx++].ToCharArray();
					string clean = "";
					for (int i = 1; i < temp.Length - 1; i++)
					{
						clean += temp[i];
					}
					string[] v = clean.Split(',');
					foreach (string input in v)
					{
						freqCounter(input, k, times);
					}
				}

				idx++;
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

		static void freqCounter(string v, string k, int times)
		{
			tuple temp = new tuple(k, v);
			if (freqDict.ContainsKey(temp))
			{
				freqDict[new tuple(k, v)] += times;
			}
			else
			{
				freqDict.Add(temp, times);
			}
		}
	}
}
