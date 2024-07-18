using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

namespace Donanim_Index_Builder
{


	class Program
	{
		private static List<(string ObjectName, string IndexName, float FragmentationPercent)> previousIndexes = new List<(string ObjectName, string IndexName, float FragmentationPercent)>();
		private static string serverName;
		private static string databaseName;
		private static string userId;
		private static string password;
		private static string connectionString;
		private static int initialTableCount = 0;
		private static int currentTableCount = 0;

		static void Main()
		{
			CollectDatabaseInfo();
			RunIndexRebuildProcess();
		}

		static void CollectDatabaseInfo()
		{
			Console.WriteLine("SQL Server adı:");
			serverName = Console.ReadLine();

			Console.WriteLine("Veritabanı adı:");
			databaseName = Console.ReadLine();

			Console.WriteLine("Kullanıcı Adı:örn;SA");
			userId = Console.ReadLine();

			Console.WriteLine("Şifresi:");
			password = Console.ReadLine();

			connectionString = $"Server={serverName};Database={databaseName};User Id={userId};Password={password};";
		}

		static void RunIndexRebuildProcess()
		{
			while (true)
			{
				using (SqlConnection connection = new SqlConnection(connectionString))
				{
					try
					{
						connection.Open();
						Console.WriteLine("SQL Server'a bağlanıldı.");

						string createTableQuery = @"
                    IF OBJECT_ID('tempdb..#IndexFragmentation') IS NOT NULL
                        DROP TABLE #IndexFragmentation;

                    CREATE TABLE #IndexFragmentation (
                        ObjectName VARCHAR(128),
                        IndexName VARCHAR(128),
                        FragmentationPercent FLOAT
                    );

                    INSERT INTO #IndexFragmentation (ObjectName, IndexName, FragmentationPercent)
                    SELECT
                        OBJECT_NAME(DPS.object_id) AS ObjectName,
                        SI.name AS IndexName,
                        DPS.avg_fragmentation_in_percent AS FragmentationPercent
                    FROM
                        sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') AS DPS
                        JOIN sys.indexes AS SI ON DPS.object_id = SI.object_id AND DPS.index_id = SI.index_id
                    WHERE
                        DPS.avg_fragmentation_in_percent > 10
                        AND SI.name IS NOT NULL;";

						using (SqlCommand command = new SqlCommand(createTableQuery, connection))
						{
							command.ExecuteNonQuery();
						}

						string selectIndexesQuery = "SELECT ObjectName, IndexName, FragmentationPercent FROM #IndexFragmentation;";

						SqlCommand selectCommand = new SqlCommand(selectIndexesQuery, connection);
						SqlDataReader reader = selectCommand.ExecuteReader();

						var currentIndexes = new List<(string ObjectName, string IndexName, float FragmentationPercent)>();

						while (reader.Read())
						{
							currentIndexes.Add((reader.GetString(0), reader.GetString(1), (float)reader.GetDouble(2)));
						}

						reader.Close();

						int totalIndexes = currentIndexes.Count;
						currentTableCount = totalIndexes;

						if (initialTableCount == 0)
						{
							initialTableCount = totalIndexes;
						}

						int currentIndex = 0;

						foreach (var index in currentIndexes)
						{
							string actionQuery;

							if (index.FragmentationPercent > 10 && index.FragmentationPercent <= 30)
							{
								actionQuery = $"ALTER INDEX [{index.IndexName}] ON [{index.ObjectName}] REORGANIZE;";
								ExecuteSqlCommand(actionQuery, connection);
								Console.WriteLine($"Reorganized index: {index.ObjectName}.{index.IndexName} with fragmentation: {index.FragmentationPercent}%");
							}
							else if (index.FragmentationPercent > 30)
							{
								actionQuery = $"ALTER INDEX [{index.IndexName}] ON [{index.ObjectName}] REBUILD;";
								ExecuteSqlCommand(actionQuery, connection);
								Console.WriteLine($"Rebuilt index: {index.ObjectName}.{index.IndexName} with fragmentation: {index.FragmentationPercent}%");
							}

							currentIndex++;
							ShowProgressBar(currentIndex, totalIndexes);
						}

						string dropTableQuery = "DROP TABLE #IndexFragmentation;";
						using (SqlCommand dropCommand = new SqlCommand(dropTableQuery, connection))
						{
							dropCommand.ExecuteNonQuery();
						}

						GenerateReport(currentIndexes);

						if (!AskForRepeatOrChangeDatabase())
						{
							break;
						}

						previousIndexes = currentIndexes;
					}
					catch (Exception ex)
					{
						Console.WriteLine("Hata: " + ex.Message);
						Console.WriteLine("Hata Ayrıntıları: " + ex.StackTrace);
					}
					finally
					{
						Console.WriteLine("Devam etmek için bir tuşa basın...");
						Console.ReadKey();
					}
				}
			}
		}

		static void ExecuteSqlCommand(string query, SqlConnection connection)
		{
			using (SqlCommand command = new SqlCommand(query, connection))
			{
				command.ExecuteNonQuery();
			}
		}

		static void ShowProgressBar(int progress, int total)
		{
			Console.CursorLeft = 0;
			Console.Write("[");
			int progressWidth = 50;
			int position = (progress * progressWidth) / total;

			for (int i = 0; i < progressWidth; i++)
			{
				if (i < position)
					Console.Write("=");
				else
					Console.Write(" ");
			}
			Console.Write($"] {progress * 100 / total}%");
		}

		static void GenerateReport(List<(string ObjectName, string IndexName, float FragmentationPercent)> currentIndexes)
		{
			Console.WriteLine("\n\nRaporlama:");
			foreach (var index in currentIndexes)
			{
				var previousIndex = previousIndexes.FirstOrDefault(i => i.ObjectName == index.ObjectName && i.IndexName == index.IndexName);
				if (previousIndex != default)
				{
					float improvement = previousIndex.FragmentationPercent - index.FragmentationPercent;
					string improvementText = improvement > 0 ? $"Azalma: {improvement}%" : $"Artış: {-improvement}%";
					Console.WriteLine($"{index.ObjectName}.{index.IndexName} - Şu anki Fragmentasyon: {index.FragmentationPercent}%. {improvementText}");
				}
				else
				{
					Console.WriteLine($"{index.ObjectName}.{index.IndexName} - Fragmentasyon: {index.FragmentationPercent}%");
				}

				string fragmentationComment = GetFragmentationComment(index.FragmentationPercent);
				Console.WriteLine(fragmentationComment);
			}

			Console.WriteLine($"\nİlk çalıştırmada düzeltilebilen tablo sayısı: {initialTableCount}");
			Console.WriteLine($"İkinci çalıştırmada kalan tablo sayısı: {currentTableCount}");
			Console.WriteLine($"İyileştirme oranı: {((initialTableCount - currentTableCount) / (float)initialTableCount) * 100}%");
		}

		static string GetFragmentationComment(float fragmentationPercent)
		{
			if (fragmentationPercent > 50)
			{
				return $"{fragmentationPercent}% fragmentasyon çok yüksek seviyededir. Bu kritik bir sorun olabilir ve performans üzerinde ciddi etkileri olabilir.";
			}
			else if (fragmentationPercent > 30)
			{
				return $"{fragmentationPercent}% fragmentasyon yüksek seviyededir. Performans sorunlarına yol açabilir.";
			}
			else if (fragmentationPercent > 10)
			{
				return $"{fragmentationPercent}% fragmentasyon orta seviyededir. Performans etkisi olabilir, ancak acil müdahale gerektirmez.";
			}
			else
			{
				return $"{fragmentationPercent}% fragmentasyon düşük seviyededir. Performans üzerinde önemli bir etkisi yoktur.";
			}
		}

		static bool AskForRepeatOrChangeDatabase()
		{
			Console.WriteLine("\nİşlemi tekrarlamak ister misiniz? (E/H):");
			string response = Console.ReadLine();
			if (response?.Trim().ToUpper() == "E")
			{
				return true;
			}
			else
			{
				Console.WriteLine("Başka bir veritabanında işlem yapmak ister misiniz? (E/H):");
				string dbResponse = Console.ReadLine();
				if (dbResponse?.Trim().ToUpper() == "E")
				{
					CollectDatabaseInfo();
					return true;
				}
			}

			return false;
		}
	}
}