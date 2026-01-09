using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using Microsoft.Win32;
using Parquet;
using Parquet.Data;
using Parquet.Schema;

namespace ParquetVisualizator
{
    public partial class MainWindow : Window
    {
        private Properties.Settings settings = Properties.Settings.Default;

        public MainWindow()
        {
            InitializeComponent();
            LoadSettings();
        }

        private void LoadSettings()
        {
            if (!string.IsNullOrEmpty(settings.FilePath))
            {
                textboxFilePath.Text = settings.FilePath;
                LoadFile(textboxFilePath.Text);
            }
        }

        private void SaveSettings()
        {
            settings.FilePath = textboxFilePath.Text;
            settings.Save();
        }

        private void textboxFilePath_TextChanged(object sender, TextChangedEventArgs e)
        {
            if (!string.IsNullOrEmpty(textboxFilePath.Text))
            {
                LoadFile(textboxFilePath.Text);
            }
        }

        private void buttonBrowse_Click(object sender, RoutedEventArgs e)
        {
            OpenFileDialog openFileDialog = new OpenFileDialog
            {
                Filter = "Fichiers Parquet et CSV (*.parquet;*.csv)|*.parquet;*.csv|Fichiers Parquet (*.parquet)|*.parquet|Fichiers CSV (*.csv)|*.csv|Tous les fichiers (*.*)|*.*",
                Title = "Sélectionner un fichier Parquet ou CSV"
            };

            if (openFileDialog.ShowDialog() == true)
            {
                textboxFilePath.Text = openFileDialog.FileName;
            }
        }

        private async void LoadFile(string filePath)
        {
            try
            {
                statusText.Text = "Chargement du fichier...";
                
                if (!File.Exists(filePath))
                {
                    textboxResult.Text = "Le fichier n'existe pas.";
                    statusText.Text = "Erreur";
                    return;
                }

                string extension = Path.GetExtension(filePath).ToLower();
                
                switch (extension)
                {
                    case ".parquet":
                        await LoadParquetFile(filePath);
                        break;
                    case ".csv":
                        LoadCsvFile(filePath);
                        break;
                    default:
                        textboxResult.Text = "Format de fichier non supporté. Veuillez sélectionner un fichier .parquet ou .csv.";
                        statusText.Text = "Erreur";
                        break;
                }
            }
            catch (Exception ex)
            {
                textboxResult.Text = $"Erreur lors du chargement du fichier : {ex.Message}";
                statusText.Text = "Erreur";
            }
        }

        private async Task LoadParquetFile(string filePath)
        {
            var result = new StringBuilder();
            
            using (var stream = File.OpenRead(filePath))
            {
                using (var reader = await ParquetReader.CreateAsync(stream))
                {
                    result.AppendLine($"Fichier Parquet : {Path.GetFileName(filePath)}");
                    result.AppendLine($"Nombre de row groups : {reader.RowGroupCount}");
                    result.AppendLine();

                    for (int i = 0; i < reader.RowGroupCount; i++)
                    {
                        using (var rowGroupReader = reader.OpenRowGroupReader(i))
                        {
                            var fields = reader.Schema.GetDataFields();
                            var dataColumns = new Parquet.Data.DataColumn[fields.Length];
                            
                            for (int j = 0; j < fields.Length; j++)
                            {
                                var field = (DataField)fields[j];
                                var columnData = await rowGroupReader.ReadColumnAsync(field);
                                dataColumns[j] = columnData;
                            }
                            
                            if (i == 0)
                            {
                                var headers = fields.Select(f => f.Name);
                                result.AppendLine($"En-têtes : {string.Join(" | ", headers)}");
                                result.AppendLine(new string('-', headers.Count() * 15));
                            }

                            for (int row = 0; row < Math.Min(10, dataColumns[0].Data.Length); row++)
                            {
                                var rowData = new List<string>();
                                foreach (var column in dataColumns)
                                {
                                    var value = column.Data.GetValue(row);
                                    rowData.Add(value?.ToString() ?? "NULL");
                                }
                                result.AppendLine(string.Join(" | ", rowData));
                            }

                            if (dataColumns[0].Data.Length > 10)
                            {
                                result.AppendLine($"... et {dataColumns[0].Data.Length - 10} autres lignes");
                            }
                        }
                    }
                }
            }

            textboxResult.Text = result.ToString();
            statusText.Text = "Fichier Parquet chargé avec succès";
        }

        private void LoadCsvFile(string filePath)
        {
            var result = new StringBuilder();
            var lines = File.ReadAllLines(filePath);
            
            result.AppendLine($"Fichier CSV : {Path.GetFileName(filePath)}");
            result.AppendLine($"Nombre de lignes : {lines.Length}");
            result.AppendLine();

            int maxLinesToShow = Math.Min(11, lines.Length);
            
            for (int i = 0; i < maxLinesToShow; i++)
            {
                result.AppendLine(lines[i]);
            }

            if (lines.Length > 11)
            {
                result.AppendLine($"... et {lines.Length - 11} autres lignes");
            }

            textboxResult.Text = result.ToString();
            statusText.Text = "Fichier CSV chargé avec succès";
        }

        private async void buttonToParquetFile_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                string filePath = textboxFilePath.Text;
                
                if (string.IsNullOrEmpty(filePath) || !File.Exists(filePath))
                {
                    MessageBox.Show("Veuillez sélectionner un fichier CSV valide.", "Erreur", MessageBoxButton.OK, MessageBoxImage.Error);
                    return;
                }

                string extension = Path.GetExtension(filePath).ToLower();
                if (extension != ".csv")
                {
                    MessageBox.Show("Veuillez sélectionner un fichier CSV pour la conversion.", "Erreur", MessageBoxButton.OK, MessageBoxImage.Error);
                    return;
                }

                string parquetFilePath = Path.ChangeExtension(filePath, ".parquet");
                
                statusText.Text = "Conversion en cours...";
                await ConvertCsvToParquet(filePath, parquetFilePath);
                
                MessageBox.Show($"Fichier converti avec succès :\n{parquetFilePath}", "Succès", MessageBoxButton.OK, MessageBoxImage.Information);
                statusText.Text = "Conversion terminée";
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Erreur lors de la conversion : {ex.Message}", "Erreur", MessageBoxButton.OK, MessageBoxImage.Error);
                statusText.Text = "Erreur de conversion";
            }
        }

        private async Task ConvertCsvToParquet(string csvFilePath, string parquetFilePath)
        {
            var lines = File.ReadAllLines(csvFilePath);
            if (lines.Length == 0) return;

            var headers = lines[0].Split(',');
            var dataRows = lines.Skip(1).ToList();

            var fields = new List<DataField>();

            for (int i = 0; i < headers.Length; i++)
            {
                fields.Add(new DataField(headers[i].Trim(), typeof(string)));
            }
            
            var schema = new ParquetSchema(fields);
            
            // Créer les colonnes de données
            var columns = new List<Parquet.Data.DataColumn>();
            var columnData = new List<string>[fields.Count];
            
            for (int i = 0; i < fields.Count; i++)
            {
                columnData[i] = new List<string>();
            }

            foreach (var line in dataRows)
            {
                var values = line.Split(',');
                
                for (int i = 0; i < Math.Min(fields.Count, values.Length); i++)
                {
                    columnData[i].Add(values[i].Trim());
                }
                
                // Compléter avec des valeurs vides si nécessaire
                for (int i = values.Length; i < fields.Count; i++)
                {
                    columnData[i].Add(string.Empty);
                }
            }

            for (int i = 0; i < fields.Count; i++)
            {
                columns.Add(new Parquet.Data.DataColumn(fields[i], columnData[i].ToArray()));
            }

            using (var fileStream = new FileStream(parquetFilePath, FileMode.Create))
            {
                using (var writer = await ParquetWriter.CreateAsync(schema, fileStream))
                {
                    using (var rowGroupWriter = writer.CreateRowGroup())
                    {
                        foreach (var column in columns)
                        {
                            await rowGroupWriter.WriteColumnAsync(column);
                        }
                    }
                }
            }
        }

        protected override void OnClosed(EventArgs e)
        {
            SaveSettings();
            base.OnClosed(e);
        }
    }
}
