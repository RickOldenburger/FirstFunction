using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
/*
    1. Use the following site to install SqlClient to make ODBC connection
    https://chanmingman.wordpress.com/2021/12/24/add-system-data-sqlclient-using-visual-studio-code/

    - Basic tutorial to connect to databases: https://www.youtube.com/watch?v=2qW1zsuJ9s0
    Fairly robust solution to connect function to database example of SQL parms and azure environment variable and setting up azure
    https://www.c-sharpcorner.com/article/develop-a-rest-api-with-azure-functions-and-sql/

    - Greate course that is an overview of functions
    https://www.youtube.com/watch?v=eS5GJkI69Qg&list=PLMWaZteqtEaLRsSynAsaS_aLzDPBUU4CV

    2. Setting up environment variables video. These are called Application settings in Azure. 
    https://www.youtube.com/watch?v=7SYwAjmdFA4
    This is a great set serries to learn azure in general (including functions)
    https://www.youtube.com/channel/UC_n9wCmDG064tZUKZF2g4Aw

    Define the "Local" environment variables
    https://eugenechiang.azurewebsites.net/2021/03/26/azure-function-get-environment-variable-using-local-settings-json/#:~:text=To%20get%20an%20environment%20variable%20or%20an%20app,works%20both%20locally%20with%20local.settings.json%20and%20in%20Azure

    3. Using Azure key vault: These are added to an environment variable as: 
    @Microsoft.KeyVault(SecretUri=<Environment variable link>)
    for example:
    @Microsoft.KeyVault(SecretUri=https://osar-dev-vault.vault.azure.net/secrets/rickodb1-database-windows-net-sqluser/c8a3d66cd53d4f0c94b97888746e7c42)
    https://blog.joaograssi.com/using-azure-key-vault-references-with-azure-functions-appservice/#:~:text=On%20the%20Azure%20Portal%2C%20go%20to%20Resource%20groups,to%20your%20Key%20Vault%20when%20the%20deployment%20finishes
    Notes: key vault access costs a little extra money and there is an alternative, see step 4 Connections strings

    4. Using connection strings (These are define in configuration just under the application setting [Environment variable])
    Note: Connection strings are encrypted at rest and transmitted over an encrypted channel. 
        Connection strings should only be used with a function app if you are using entity framework. 
        For other scenarios use App Settings.
    These are then accessed like an environment variable except you prepend the string with the typ of connection you are using current options are:
    MySQL:      MySQL_<name>
    SQLServer:  SQLServer_<name>
    SQLAzure:   SQLAzure_<name>
    PostgreSQL: PostgreSQL_<name>
    Custom:     Custom_<name>

    Final note on steps 3 - 4, these environment variables are NOT case sensitive. And it seems like Azure uses either all upper case or Camel case,
    so for our variables we will user lower case

    user:     sqluser
    Password: pswd1234!

    May want to review https://www.youtube.com/watch?v=l3SBq7L13Mk for azure static webapps   Azure Static Web Apps [2 of 16]
    path to static web apps C:\Users\ricko\AppData\Roaming\npm\node_modules\@azure\static-web-apps-cli
    swa start build --api ../api

    link storage account to azure function
    https://techcommunity.microsoft.com/t5/apps-on-azure-blog/secure-storage-account-linked-to-function-app-with-private/ba-p/2644772
    https://www.youtube.com/watch?v=lxBm-wlTqeQ

    need to add 
      "AzureWebJobsStorage": "UseDevelopmentStorage=true", to local.settings.json if one wants to use a storage account
      <PackageReference Include="Microsoft.Azure.WebJobs.Extensions.Storage" Version="5.0.0"/> to FirstFunction.csproj

    video on securely calling function from website will watch more but may need 
    https://www.youtube.com/watch?v=uST0CyqRIHA

    https://www.youtube.com/watch?v=XaS_p5D1llg using OidcAuthentication need to study further
*/
using System.Collections.Generic;

//using Microsoft.Azure.WebJobs.Extensions.Storage;
using System.Data;
using System.Data.SqlClient;
namespace My.Function
{
    public static class HttpTrigger1
    {
        [FunctionName("Options1")]
        public static async Task<IActionResult> getOptions(
            [HttpTrigger(AuthorizationLevel.Anonymous, "options", Route = "")] HttpRequest req, 
            //[Queue("outputqueue"), StorageAccount("UseDevelopmentStorage")] ICollector<string> msg,
            ILogger log
            )
        {
            log.LogInformation("Get options.");

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();

            string result = "Access-Control-Allow-Methods: OPTIONS, POST, GET";

            string responseMessage = (result);
            return new OkObjectResult(responseMessage);
        }

        [FunctionName("HttpTrigger1")]
        public static async Task<IActionResult> run1(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = "read")] HttpRequest req, 
            //[Queue("outputqueue"), StorageAccount("UseDevelopmentStorage")] ICollector<string> msg,
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

            string result = "";
            
            await Task.Run(() => getData(ref result, name, log));

            string connStr = System.Environment.GetEnvironmentVariable("connection_string", EnvironmentVariableTarget.Process);
            string password = System.Environment.GetEnvironmentVariable("CustomCONNSTR_password", EnvironmentVariableTarget.Process);
            string user = System.Environment.GetEnvironmentVariable("CustomCONNSTR_user", EnvironmentVariableTarget.Process);

            string responseMessage = (string.IsNullOrEmpty(result) || result.Trim() == "")
                ? $"This HTTP triggered function executed successfully. However, no data as returned.\nconnStr: {connStr}\npassword: {password}\nuser: {user}"
                : $"Result:\n{result}\nThis HTTP triggered function executed successfully.\nconnStr: {connStr}\npassword: {password}\nuser: {user}";

            //msg.Add($"Result: {result}");

            return new OkObjectResult(responseMessage);
        }

        [FunctionName("HttpTrigger2")]
        public static async Task<IActionResult> run2(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = "all")] HttpRequest req, 
            //[Queue("outputqueue"), StorageAccount("UseDevelopmentStorage")] ICollector<string> msg,
            ILogger log
            )
        {
            log.LogInformation("Get ALL Query.");

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();

            string result = "";   

            await Task.Run(() => getData(ref result, string.Empty, log));

            string responseMessage = (result);
            return new OkObjectResult(responseMessage);
        }

        public static void getData(ref string result, string name, ILogger log)
        {
            string connStr = System.Environment.GetEnvironmentVariable("connection_string", EnvironmentVariableTarget.Process);
            string password = System.Environment.GetEnvironmentVariable("CustomCONNSTR_password", EnvironmentVariableTarget.Process);
            string user = System.Environment.GetEnvironmentVariable("CustomCONNSTR_user", EnvironmentVariableTarget.Process);
            
            string query;

            //"Server=tcp:rickodb1.database.windows.net,1433;Initial Catalog=db1;Persist Security Info=False;User ID=sqluser;Password=pswd1234!;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;";
            
            if (name == string.Empty)
                query = "SELECT * FROM dbo.YoutubeLinks FOR JSON AUTO";
            else
                query = "SELECT * FROM dbo.YoutubeLinks WHERE name=@Name FOR JSON AUTO; --, Without_Array_Wrapper";

            DataTable dt = new DataTable();
            try
            {
                connStr = connStr.Replace("{user}", user).Replace("{password}", password);
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
                result = dt.Rows[0].Field<string>(0);

        }
    }
}
