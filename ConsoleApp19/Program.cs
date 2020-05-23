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
		static int N = 395;
		static Dictionary<tuple, int> freqDict = new Dictionary<tuple, int>();
		static Dictionary<string, int> maxDict = new Dictionary<string, int>();
		static Dictionary<tuple, float> qfDict = new Dictionary<tuple, float>();
		static Dictionary<tuple, double> idfCatDict = new Dictionary<tuple, double>();

		static SQLiteConnection m_dbConnection;
		static void Main(string[] args)
		{

			createDatabase();
			rundbCommands();
			workloadLoad();
			qf();

			string command = "Select * from autompg";
			SQLiteCommand henk = new SQLiteCommand(command, m_dbConnection);
			SQLiteDataReader r = henk.ExecuteReader();
			while (r.Read())
				Console.WriteLine("id: " + r["id"] + " name: " + r["model"]);

			while (true)
			{
				string s = Console.ReadLine();
				SQLiteCommand com = new SQLiteCommand(s, m_dbConnection);
				SQLiteDataReader reader = com.ExecuteReader();
				while (reader.Read())
				{
					object[] array = new object[1];
					reader.GetValues(array);

					for (int i = 0; i < array.Length; i++)
					{
						Console.Write(reader.GetName(i) + ": " + array[i] + ", ");
					}
					Console.WriteLine();
				}
			}
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
				freqDict[temp] += times;
			}
			else
			{
				freqDict.Add(temp, times);
			}

			if (maxDict.ContainsKey(k))
			{
				if (maxDict[k] < freqDict[temp])
				{
					maxDict[k] = freqDict[temp];
				}
			}
			else
			{
				maxDict.Add(k, freqDict[temp]);
			}
		}
		//  RQFk(v) / RQFMAXk
		static void qf()
		{
			foreach (KeyValuePair<tuple, int> t in freqDict)
			{
				float qfv = (float)t.Value / maxDict[t.Key.column];
				qfDict.Add(t.Key, qfv);
				Console.WriteLine(t.Key.column + " " + t.Key.value + " " + qfv);
			}
		}

		static void idf()
		{
			string sql = "";
			string[] clNames = {"mpg", "cylinders", "displacement", "horsepower", "weight", "acceleration", "model_year", "origin", "brand", "model", "type"};
			for (int i = 0; i < 10; i++)
			{
				if (i < 9)
				{

				}
				else
				{
					sql = "select distinct " + clNames[i] + " from autompg";
				}

				SQLiteCommand command = new SQLiteCommand(sql, m_dbConnection);
				SQLiteDataReader reader = command.ExecuteReader();
				while (reader.Read())
				{
					string val = reader[clNames[i]].ToString();
					sql = "select count(*) from autompg where " + clNames[i] + " = '" + val + "'";
					SQLiteCommand com = new SQLiteCommand(sql, m_dbConnection);
					int freq = (int)com.ExecuteScalar();
					idfCatDict.Add(new tuple(clNames[i], val), Math.Log10(N/freq));
				}
			}
		}
	}
}
