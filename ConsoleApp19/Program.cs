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

	public struct tupleInt
	{
		public int query;
		public int times;
		public tupleInt(int k, int v)
		{
			query = k;
			times = v;
		}
	}

	class Program
	{
		static int N = 395;
		static int id = 0;
		static Dictionary<tuple, int> freqDict = new Dictionary<tuple, int>();
		static Dictionary<string, int> maxDict = new Dictionary<string, int>();
		static Dictionary<tuple, double> qfDict = new Dictionary<tuple, double>();
		static Dictionary<tuple, double> idfDict = new Dictionary<tuple, double>();
		static Dictionary<string, List <tupleInt>> jaqDict = new Dictionary<string, List<tupleInt>>();


		static SQLiteConnection m_dbConnection;
		static void Main(string[] args)
		{

			createDatabase();
			rundbCommands();
			workloadLoad();
			qf();
			idf();
			fillMetaDB();

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
					string comb = trimString(q[idx++]);
					
					string[] v = comb.Split(',');


					foreach (string input in v)
					{
						if (jaqDict.ContainsKey(input))
							jaqDict[input].Add(new tupleInt(id, times));
						else
							jaqDict.Add(input, new List<tupleInt> { new tupleInt(id, times) });
						freqCounter(input, k, times);
					}
					id++;
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
			v = trimString(v);

            if(double.TryParse(v, out double test))
            {
                return;
            }
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
				double qfv = (double)t.Value / maxDict[t.Key.column];
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
				sql = "select distinct " + clNames[i] + " from autompg";
				SQLiteCommand command = new SQLiteCommand(sql, m_dbConnection);
				SQLiteDataReader reader = command.ExecuteReader();

				if (i < 8)
				{
					double h = calculateStdDev(clNames[i]) * 1.06 * Math.Pow(N, -1/5.0);
					while (reader.Read())
					{
						double denom = 0;
						double t = double.Parse(reader[clNames[i]].ToString());
						sql = "select " + clNames[i] + " from autompg";
						SQLiteCommand com = new SQLiteCommand(sql, m_dbConnection);
						SQLiteDataReader r = com.ExecuteReader();
						while (r.Read())
						{
							double ti = double.Parse(r[clNames[i]].ToString());
							double diff = ti - t;
							denom += Math.Exp(-0.5 * (diff / h) * (diff / h));
						}
			
						idfDict.Add(new tuple(clNames[i], t.ToString()), Math.Log10(N / denom));
					}
				}
				//else
				//{

				//	while (reader.Read())
				//	{
				//		string val = reader[clNames[i]].ToString();
				//		sql = "select count(*) from autompg where " + clNames[i] + " = '" + val + "'";
				//		SQLiteCommand com = new SQLiteCommand(sql, m_dbConnection);
				//		int freq = (int)com.ExecuteScalar();
				//		idfDict.Add(new tuple(clNames[i], val), Math.Log10(N / freq));
				//	}
				//}

			}
		}

		static double calculateStdDev(string col)
		{
			double sum = 0;
			string avgsql = "Select avg(" + col + ") from autompg";
			SQLiteCommand command = new SQLiteCommand(avgsql, m_dbConnection);
			double avg = (double)command.ExecuteScalar();
			string sql = "Select " + col + " from autompg";
			SQLiteCommand com = new SQLiteCommand(sql, m_dbConnection);
			SQLiteDataReader r = com.ExecuteReader();
			
			while (r.Read())
			{
				string temp = r[col].ToString();
				sum += ((double.Parse(temp)) - avg) * ((double.Parse(temp)) - avg);
			}
			return Math.Sqrt(sum / (N-1));
		}

		static void createMetaDB()
		{

		}

		static void fillMetaDB()
		{
			StreamWriter sr;
			sr = new StreamWriter("../../metaload.txt");


			foreach (KeyValuePair<tuple, double> t in idfDict)
			{
				sr.WriteLine("INSERT INTO idfqf VALUES (" + t.Key.column + ", " + t.Key.value + ", " + t.Value +  ")" );
			}

			foreach (KeyValuePair<tuple, double> t in qfDict)
			{
                sr.WriteLine("INSERT INTO idfqf VALUES (" + t.Key.column + ", " + t.Key.value + ", " + t.Value + ")");
            }

			calculateJaqCof(sr);
            sr.Close();
		}

		static string trimString(string v)
		{
			v = v.Remove(0, 1);
			v = v.Remove(v.Length - 1, 1);
			return v;
		}

		static void calculateJaqCof(StreamWriter sr)
		{
			foreach (KeyValuePair<string, List<tupleInt>> t0 in jaqDict)
			{
				foreach (KeyValuePair<string, List<tupleInt>> t1 in jaqDict)
				{
					
					if (t0.Equals(t1))
					{
						break;
					}

					int i = 0;
					int j = 0;
					int total = 0;
					int combo = 0;

					while (i < t0.Value.Count || j < t1.Value.Count)
					{
						if (i < t0.Value.Count && j < t1.Value.Count)
						{
							if (t0.Value[i].query == t1.Value[j].query)
							{
								total += t0.Value[i].times;
								combo += t0.Value[i].times;
								i++;
								j++;
							}
							else if (t0.Value[i].query < t1.Value[j].query)
							{
								total += t0.Value[i].times;
								i++;
							}
							else
							{
								total += t1.Value[j].times;
								j++;
							}
						}
						else if (i < t0.Value.Count)
						{
							total += t0.Value[i].times;
							i++;
						}
						else
						{
							total += t1.Value[j].times;
							j++;
						}
					}

					sr.WriteLine("INSERT INTO jacquard VALUES (" + t0.Key + ", " + t1.Key + ", " + ((double) combo) / total + ")" );
				}
			}
		}
	}
}
