using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

// Use the following site to install SqlClient to make ODBC connection
// https://chanmingman.wordpress.com/2021/12/24/add-system-data-sqlclient-using-visual-studio-code/
//
// Basic tutorial to connect to databases: https://www.youtube.com/watch?v=2qW1zsuJ9s0
// Fairly robust solution to connect function to database example of SQL parms and azure environment variable and setting up azure
// https://www.c-sharpcorner.com/article/develop-a-rest-api-with-azure-functions-and-sql/
//
// Greate course that is an overview of functions
// https://www.youtube.com/watch?v=eS5GJkI69Qg&list=PLMWaZteqtEaLRsSynAsaS_aLzDPBUU4CV

// user:     sqluser
// Password: pswd1234!
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
namespace My.Function
{
    public static class HttpTrigger1
    {
        [FunctionName("HttpTrigger1")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log
            )
        {
            log.LogInformation("Get Name Query.");

            string name = req.Query["name"];

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            name = name ?? data?.name;

            if (string.IsNullOrEmpty(name) || name == "")
            {
                log.LogWarning("must provide a name to search for.");
                return new BadRequestObjectResult("Expecting name with a value in the request body.");
            }

            string result = getData(name, log);

            string responseMessage = (string.IsNullOrEmpty(result) || result.Trim() == "")
                ? "This HTTP triggered function executed successfully. However, no data as returned."
                : $"Result:\n{result}\nThis HTTP triggered function executed successfully.";

            return new OkObjectResult(responseMessage);
        }

        public static string getData(string name, ILogger log)
        {
            string connStr = "Server=tcp:rickodb1.database.windows.net,1433;Initial Catalog=db1;Persist Security Info=False;User ID=sqluser;Password=pswd1234!;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;";
            string query = "SELECT * FROM dbo.YoutubeLinks WHERE name=@Name FOR JSON AUTO; --, Without_Array_Wrapper";

            DataTable dt = new DataTable();
            try
            {
                using (SqlConnection connection = new SqlConnection(connStr))
                {
                    connection.Open();
                    SqlCommand command = new SqlCommand(query, connection);
                    command.Parameters.AddWithValue("@Name", name);
                    SqlDataAdapter da = new SqlDataAdapter(command);
                    da.Fill(dt);
                }
            }
            catch (Exception e)
            {
                log.LogError(e.ToString());
            }

            if (dt.Rows.Count > 0)
                return dt.Rows[0].Field<string>(0);
            return "";
        }
    }
}
