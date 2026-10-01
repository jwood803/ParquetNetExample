using System.Globalization;
using CsvHelper;
using Parquet;
using Parquet.Data;
using Parquet.Schema;
using Parquet.Serialization;
using ParquetNetSample;

// 1. Read a Parquet file in one line with the high-level API
var sentenceSimilaritiesHighLevel = await ParquetSerializer.DeserializeAsync<SentenceSimilarity>(
    "train.parquet", new ParquetSerializerOptions { PropertyNameCaseInsensitive = true });

Console.WriteLine($"High-level API read {sentenceSimilaritiesHighLevel.Count:N0} rows");
foreach (var row in sentenceSimilaritiesHighLevel.Take(3))
{
    Console.WriteLine($"  {row.Score:0.00} | {row.Sentence1} | {row.Sentence2}");
}

// 2. Read with full control using the low-level API
List<SentenceSimilarity> sentenceSimilarities = new();

using (var stream = File.OpenRead("train.parquet"))
using (var reader = await ParquetReader.CreateAsync(stream))
{
    Console.WriteLine(reader.Schema);

    DataField[] dataFields = reader.Schema.GetDataFields();

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
                    : Convert.ToDouble(similarity.Data.GetValue(j))
            });
        }
    }
}

Console.WriteLine($"Low-level API read {sentenceSimilarities.Count:N0} rows");

// 3. Save the data to a CSV file with CsvHelper
using (var csvStream = new StreamWriter("similarity.csv"))
using (var csv = new CsvWriter(csvStream, CultureInfo.InvariantCulture))
{
    csv.WriteRecords(sentenceSimilarities);
}

Console.WriteLine("Wrote similarity.csv");

// 4. Write a Parquet file in one line with the high-level API
var newRows = new List<SentenceSimilarity>
{
    new() { Sentence1 = "A plane is taking off.", Sentence2 = "An airplane is taking off.", Score = 0.95 },
    new() { Sentence1 = "A man is playing guitar.", Sentence2 = "A person is playing music.", Score = 0.65 },
    new() { Sentence1 = "A dog is running in the park.", Sentence2 = "A woman is cooking dinner.", Score = 0.10 },
};

await ParquetSerializer.SerializeAsync(newRows, "new_similarity.parquet");

Console.WriteLine("Wrote new_similarity.parquet (high-level API)");

// 5. Write a Parquet file column by column with the low-level API
// Score is nullable (double?) to match the SentenceSimilarity class. If the schema says double but the
// class says double?, reading the file back fails with "nullability ... is incompatible".
var schema = new ParquetSchema(
    new DataField<string>("Sentence1"),
    new DataField<string>("Sentence2"),
    new DataField<double?>("Score")
);

var newSen1Data = new[] { "A plane is taking off.", "A man is playing guitar.", "A dog is running in the park." };
var newSen2Data = new[] { "An airplane is taking off.", "A person is playing music.", "A woman is cooking dinner." };
var newScoreData = new double?[] { 0.95, 0.65, 0.10 };

// File.Create, not File.OpenWrite: OpenWrite doesn't truncate an existing file, so writing a smaller
// file over a larger one leaves stale bytes at the end and can corrupt the Parquet file.
// The block makes sure the writer is disposed (and the file finished) before we read it back.
using (var writeStream = File.Create("new_similarity_lowlevel.parquet"))
{
    await using ParquetWriter writer = await ParquetWriter.CreateAsync(schema, writeStream);
    using ParquetRowGroupWriter rowGroupWriter = writer.CreateRowGroup();

    await rowGroupWriter.WriteColumnAsync(new DataColumn((DataField)schema.Fields[0], newSen1Data));
    await rowGroupWriter.WriteColumnAsync(new DataColumn((DataField)schema.Fields[1], newSen2Data));
    await rowGroupWriter.WriteColumnAsync(new DataColumn((DataField)schema.Fields[2], newScoreData));
}

var check = await ParquetSerializer.DeserializeAsync<SentenceSimilarity>(
    "new_similarity_lowlevel.parquet", new ParquetSerializerOptions { PropertyNameCaseInsensitive = true });

Console.WriteLine($"Wrote new_similarity_lowlevel.parquet (low-level API) and read back {check.Count} rows");
