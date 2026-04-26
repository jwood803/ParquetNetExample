using CsvHelper;
using Parquet;
using Parquet.Data;
using Parquet.Schema;
using Parquet.Serialization;
using ParquetNetSample;

// Writing parquet files
var schema = new ParquetSchema(
    new DataField<string>("Sentence1"),
    new DataField<string>("Sentence2"),
    new DataField<double>("Score")
);

var newSen1Data = new[] { "A plane is taking off.", "A man is playing guitar.", "A dog is running in the park." };
var newSen2Data = new[] { "An airplane is taking off.", "A person is playing music.", "A woman is cooking dinner." };
var newScoreData = new[] { 0.95, 0.65, 0.10 };

using var writeStream = File.OpenWrite("new_similarity.parquet");
await using ParquetWriter writer = await ParquetWriter.CreateAsync(schema, writeStream);
using ParquetRowGroupWriter rowGroupWriter = writer.CreateRowGroup();

await rowGroupWriter.WriteColumnAsync(new DataColumn((DataField)schema.Fields[0], newSen1Data));
await rowGroupWriter.WriteColumnAsync(new DataColumn((DataField)schema.Fields[1], newSen2Data));
await rowGroupWriter.WriteColumnAsync(new DataColumn((DataField)schema.Fields[2], newScoreData));

// Low-level API to read Parquet files
using var stream = File.OpenRead("train.parquet");
using var reader = await ParquetReader.CreateAsync(stream);

Console.WriteLine(reader.Schema);

DataField[] dataFields = reader.Schema.GetDataFields();
List<SentenceSimilarity> sentenceSimilarities = new();

for (int i = 0; i < reader.RowGroupCount; i++)
{
    using ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(i);

    var sen1 = await rowGroupReader.ReadColumnAsync(dataFields[0]);
    var sen2 = await rowGroupReader.ReadColumnAsync(dataFields[1]);
    var similarity = await rowGroupReader.ReadColumnAsync(dataFields[2]);

    int rowCount = sen1.Data.Length;

    for (int j = 0; j < rowCount; j++)
    {
        sentenceSimilarities.Add(new()
        {
            Sentence1 = sen1.Data.GetValue(j)?.ToString(),
            Sentence2 = sen2.Data.GetValue(j)?.ToString(),
            Score = similarity.Data.GetValue(j) is null
                ? 0
                : Convert.ToSingle(similarity.Data.GetValue(j))
        });
    }
}

// High-level API to read Parquet files
var sentenceSimilaritiesHighLevel = await ParquetSerializer.DeserializeAsync<SentenceSimilarity>("train.parquet", new ParquetSerializerOptions { PropertyNameCaseInsensitive = true });

// Save to CSV
using var writerHighLevel = new StreamWriter("similarity.csv");
using var csv = new CsvWriter(writerHighLevel, System.Globalization.CultureInfo.InvariantCulture);

csv.WriteRecords(sentenceSimilarities);
