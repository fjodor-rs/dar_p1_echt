using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SQLite;
using System.IO;

namespace ConsoleApp19
{

	public struct tuple<T, S>
	{
		public T first;
		public S second;
		public tuple(T f, S s)
		{
			first = f;
			second = s;
		}
	}


	class Program
	{
		static int N = 395;
		static int id = 0;

		static Dictionary<string, double> hColumnDict = new Dictionary<string, double>();
		static Dictionary<tuple<string, string>, int> freqDict = new Dictionary<tuple<string, string>, int>();
		static Dictionary<string, int> maxDict = new Dictionary<string, int>();
		static Dictionary<tuple<string, string>, double> qfDict = new Dictionary<tuple<string, string>, double>();
		static Dictionary<tuple<string, string>, double> idfDict = new Dictionary<tuple<string, string>, double>();
		static Dictionary<string, List<tuple<int, int>>> jaqDict = new Dictionary<string, List<tuple<int, int>>>();


        static string[] numericalNames = { "mpg", "cylinders", "displacement", "horsepower", "weight", "acceleration", "model_year", "origin" };
        static string[] categoricalNames = {"brand", "model", "type" };

        static SQLiteConnection meta_dbConnection;
		static SQLiteConnection m_dbConnection;
		static void Main(string[] args)
		{
			createDatabase();
			rundbCommands("database.txt", m_dbConnection);
			workloadLoad();
			addQftoDict();
			idf();
			createMetaDB();
			fillMetaDB();

            Console.WriteLine("Alles is geladen, je kan nu queries typen");

			while (true)
			{
				string s = Console.ReadLine();
                queryParser(s);
			}
		}

		public static void createDatabase()
		{
			SQLiteConnection.CreateFile("MyDatabase.sqlite");
			m_dbConnection = new SQLiteConnection("Data Source=MyDatabase.sqlite;Version=3;");
			m_dbConnection.Open();
		}

		public static void rundbCommands(string fileName, SQLiteConnection m_dbConnection)
		{
			try
			{
				using (StreamReader sr = new StreamReader("../../" + fileName))
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
							jaqDict[input].Add(new tuple<int, int>(id, times));
						else
							jaqDict.Add(input, new List<tuple<int, int>> { new tuple<int, int>(id, times) });
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

			if (double.TryParse(v, out double test))
			{
				return;
			}
			tuple<string, string> temp = new tuple<string, string>(k, v);
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
		static void addQftoDict()
		{
			foreach (KeyValuePair<tuple<string, string>, int> t in freqDict)
			{
				double qfv = (double)t.Value / maxDict[t.Key.first];
				qfDict.Add(t.Key, qfv);
			}
		}

		static void idf()
		{

			string sql = "";
			for (int i = 0; i < 8; i++)
			{
				sql = "select distinct " + numericalNames[i] + " from autompg";
				SQLiteCommand command = new SQLiteCommand(sql, m_dbConnection);
				SQLiteDataReader reader = command.ExecuteReader();
			
				double h = calculateStdDev(numericalNames[i]) * 1.06 * Math.Pow(N, -1 / 5.0);
				hColumnDict.Add(numericalNames[i], h);

				while (reader.Read())
				{
					double denom = 0;
					double t = double.Parse(reader[numericalNames[i]].ToString());
					sql = "select " + numericalNames[i] + " from autompg";
					SQLiteCommand com = new SQLiteCommand(sql, m_dbConnection);
					SQLiteDataReader r = com.ExecuteReader();
					while (r.Read())
					{
						double ti = double.Parse(r[numericalNames[i]].ToString());
						double diff = ti - t;
						denom += Math.Exp(-0.5 * (diff / h) * (diff / h));
					}

					idfDict.Add(new tuple<string, string>(numericalNames[i], t.ToString()), Math.Log10(N / denom));
				}	
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
			return Math.Sqrt(sum / (N - 1));
		}

		static void createMetaDB()
		{
			SQLiteConnection.CreateFile("MetaDatabase.sqlite");
			meta_dbConnection = new SQLiteConnection("Data Source=MetaDatabase.sqlite;Version=3;");
			meta_dbConnection.Open();
			rundbCommands("metadb.txt", meta_dbConnection);
		}

		static void fillMetaDB()
		{
			StreamWriter sr;
			sr = new StreamWriter("../../metaload.txt");
			string sql = "";
			SQLiteCommand command;

			foreach (KeyValuePair<tuple<string, string>, double> t in idfDict)
			{

				sql = "INSERT INTO idfqf VALUES ('" + t.Key.first + "', '" + t.Key.second + "', '" + doubleToString(t.Value) + "')";
				sr.WriteLine(sql);
				command = new SQLiteCommand(sql, meta_dbConnection);
				command.ExecuteNonQuery();
			}

			foreach (KeyValuePair<tuple<string, string>, double> t in qfDict)
			{
				sql = "INSERT INTO idfqf VALUES ('" + t.Key.first + "', '" + t.Key.second + "', '" + doubleToString(t.Value) + "')";
				sr.WriteLine(sql);
				command = new SQLiteCommand(sql, meta_dbConnection);
				command.ExecuteNonQuery();
			}

			calculateJaqCof(sr);
			sr.Close();
		}

		static string doubleToString(double val)
		{
			return val.ToString().Replace(',', '.');
		}

		static string trimString(string v)
		{
			v = v.Remove(0, 1);
			v = v.Remove(v.Length - 1, 1);
			return v;
		}

		static void calculateJaqCof(StreamWriter sr)
		{
			foreach (KeyValuePair<string, List<tuple<int, int>>> t0 in jaqDict)
			{
				foreach (KeyValuePair<string, List<tuple<int, int>>> t1 in jaqDict)
				{

					if (t0.Equals(t1))
					{
						string sql = "INSERT INTO jacquard VALUES (" + t0.Key + ", " + t1.Key + ", '" + doubleToString(1.0) + "')";
						sr.WriteLine(sql);
						SQLiteCommand command = new SQLiteCommand(sql, meta_dbConnection);
						command.ExecuteNonQuery();
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
							if (t0.Value[i].first == t1.Value[j].first)
							{
								total += t0.Value[i].second;
								combo += t0.Value[i].second;
								i++;
								j++;
							}
							else if (t0.Value[i].first < t1.Value[j].first)
							{
								total += t0.Value[i].second;
								i++;
							}
							else
							{
								total += t1.Value[j].second;
								j++;
							}
						}
						else if (i < t0.Value.Count)
						{
							total += t0.Value[i].second;
							i++;
						}
						else
						{
							total += t1.Value[j].second;
							j++;
						}
					}
					if (combo != 0)
					{
						string sql = "INSERT INTO jacquard VALUES (" + t0.Key + ", " + t1.Key + ", '" + doubleToString(((double)combo) / total) + "')";
						sr.WriteLine(sql);
						SQLiteCommand command = new SQLiteCommand(sql, meta_dbConnection);
						command.ExecuteNonQuery();
					}
				}
			}
		}

		static void queryParser(string query)
		{
			query = query.ToLower();
			int k = 10;
			int start_i = 0;
			int j = 0;
			string split = "where ";
			string start = query.Substring(0, query.IndexOf(split) + split.Length);
			string end = query.Substring(query.IndexOf(split) + split.Length);
			end = end.Replace(",", "");
			end = end.TrimEnd(';');
			string[] terms = end.Split();
			int strLength = terms.Length / 3;
			string[] columns; 
			string[] values;

			if (terms[0] == "k")
			{
				k = int.Parse(terms[2]);
				start_i = 3;
				columns = new string[strLength - 1];
				values = new string[strLength - 1];
			}
			else
			{
				columns = new string[strLength];
				values = new string[strLength];
			}

			for (int i = start_i; i < terms.Length; i++)
			{
				columns[j] = terms[i];
				i += 2;
				values[j] = terms[i];
				j++;
			}

            try
            {
                tuple<int, double>[] topKTuples = calculateSim(columns, values, k);
                printTopK(topKTuples, start);
            }
            catch(Exception e)
            {
                Console.WriteLine(e.Message);
                Console.WriteLine("Fjodor's customer service: Please try again good sir/mam.");
            }
		}

		static tuple<int, double>[] calculateSim(string[] columns, string[] values, int k)
		{

			string sql = "select * from autompg";

			SQLiteCommand com = new SQLiteCommand(sql, m_dbConnection);
            
            SQLiteDataReader reader = com.ExecuteReader();

			tuple<int, double>[] topKTuples = new tuple<int, double>[k] ;
			for (int i = 0; i < k; i++)
			{
				topKTuples[i] = new tuple<int, double>(-1, 0);
			}

            Dictionary<int, double> MissingAttributesValues = new Dictionary<int, double>();

			while (reader.Read())
			{
				double simScore = 0;
				for (int i = 0; i < values.Length; i++)
				{
					string sqlMeta = "select idfqf from idfqf where column = '" + columns[i] + "' and value = " + values[i];
					SQLiteCommand metaCom = new SQLiteCommand(sqlMeta, meta_dbConnection);
					 SQLiteDataReader metaReader = metaCom.ExecuteReader();
					if (metaReader.Read())
					{
						// numerical similarity
						if (double.TryParse(values[i], out double q))
						{
							// t value van de tuple, q value van de query
							double innerproduct = ((double.Parse(reader[columns[i]].ToString()) - q) / hColumnDict[columns[i]]) * ((double.Parse(reader[columns[i]].ToString()) - q) / hColumnDict[columns[i]]);
							simScore += Math.Exp(-0.5 * innerproduct) * (double) metaReader.GetValue(0) ;
						}

						// categorical similarity
						else
						{
							string sqlJaq = "select jacq from jacquard where value_1 = " + values[i] + " and value_2 = '" + reader[columns[i]] + "'";
							SQLiteCommand jaqCom = new SQLiteCommand(sqlJaq, meta_dbConnection);
							SQLiteDataReader jaqReader = jaqCom.ExecuteReader();
							if (jaqReader.Read())
							{
								simScore += (double) jaqReader.GetValue(0) * (double)metaReader.GetValue(0);
							}
						}
					}
				}
                double missingscore = 0;
                for (int i = 0; i < categoricalNames.Length; i++)
                {
                    string sqlMeta = "select idfqf from idfqf where column = '" + categoricalNames[i] + "' and value = '" + reader[categoricalNames[i]] + "'";
                    SQLiteCommand metaCom = new SQLiteCommand(sqlMeta, meta_dbConnection);
                    SQLiteDataReader metaReader = metaCom.ExecuteReader();
                    if (metaReader.Read())
                    {
                        missingscore += Math.Log10((double)metaReader.GetValue(0));
                    }
                }
                MissingAttributesValues.Add(int.Parse(reader["id"].ToString()), missingscore);
				for (int i = 0; i < k; i++)
				{
					if (topKTuples[i].first == -1)
					{
						topKTuples[i] = new tuple<int, double>(int.Parse(reader["id"].ToString()), simScore);
						break;
					}

                    if(topKTuples[i].second == simScore)
                    {
                        if(MissingAttributesValues[topKTuples[i].first] < MissingAttributesValues[int.Parse(reader["id"].ToString())])
                        {
                            for (int j = k - 1; j > i; j--)
                            {
                                topKTuples[j] = topKTuples[j - 1];
                            }
                            topKTuples[i] = new tuple<int, double>(int.Parse(reader["id"].ToString()), simScore);
                            break;
                        }
                    }
                    
					if (topKTuples[i].second < simScore)
					{
						for (int j = k-1; j > i; j--)
						{
							topKTuples[j] = topKTuples[j-1];
						}
						topKTuples[i] = new tuple<int, double>(int.Parse(reader["id"].ToString()), simScore);
						break;
					}
				}
			}

			return topKTuples;
		}

		static void printTopK(tuple<int, double>[] topKTuples, string start)
		{
			string sql = "";
			for (int i = 0; i < topKTuples.Length; i++)
			{
				sql = start + "id = '";
				sql += topKTuples[i].first + "'";

				SQLiteCommand com = new SQLiteCommand(sql, m_dbConnection);
				SQLiteDataReader reader = com.ExecuteReader();
				if (reader.Read())
				{

					object[] array = new object[12] { "", "", "", "", "", "", "", "", "", "", "", "" };
					reader.GetValues(array);
					for (int j = 0; j < array.Length; j++)
					{
						if (array[j].ToString() == "")
							break;
						Console.Write(reader.GetName(j) + ": " + array[j] + ", ");
					}

					Console.WriteLine();
				}
			}
		}
	}
}
