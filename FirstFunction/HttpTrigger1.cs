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
 I. Setting up your environment
    - Setting up azure tools
    https://www.youtube.com/watch?v=crI3SA0IadE
    This video has you install azure tools. Basically just search for "Azure Tools" and install.
    The utility will install the following:
        Azure App Service, Azure Functions, Azure Storage, Azure Databases,
        Azure Virtual Machines, Azure Resources, Azure CLI Tools, 
        Azure Resource Manager (ARM) Tools, Bicep, Docker
        Azure Piplines, ARM Template Viewer

    In addition, you will want to install:
        Azure Terraform, C# for Visual Studio Code, SQL Database Projects, SQL Server (mssql),
        Azurite
       
    NuGet Packeges used for this example install:
        NuGet Package Manager GUI. This will allow you to search for and add packages using the command pallet
        packages I am using:
        Microsoft.NET.Sdk.Functions
        Microsoft.Extensions.DependencyInjection
        System.Data.SqlClient
        Microsoft.Azure.WebJobs.Extensions.Storage   --> will use when adding a storage account

    -Starting azurite:
        Once azurite is installed you can edit the file you can run: Microsoft Azure Storage emulator from the start menu

        This will attempt to start the storage emulator. The following commands can be executed on the dos prompt:
        azurestorageemulator start
        azurestorageemulator stop
        azurestorageemulator status

        If the proccess fails to execute, you probably hjave an IP address conflict. edit the file: AzureStorageEmulator.exe.config
        in C:\Program Files (x86)\Microsoft SDKs\Azure\Storage Emulator
        to change to an open IP address

        You can use:
         netstat -aon to get a list of all the processes tied to ports to find three available ones.
         netstat -nba | FINDSTR "LISTEN"
         tasklist | findstr /I process_name
         taskkill /IM process_name.exe
         taskkill /PID process_id

         Additional information on processes
         https://www.shellhacks.com/windows-taskkill-kill-process-by-pid-name-port-cmd/

        Found an issue with Azure Storage "Local Emulator" not letting you change the ports the Local Emulator uses.
        git hub location for Azure source: https://github.com/microsoft/vscode-azurestorage/
        Specific object that contains function that is preventing the alteration of a local account.
        https://github.com/microsoft/vscode-azurestorage/blob/main/src/tree/AttachedStorageAccountsTreeItem.ts


II. Courses on Azure basics
    - This set of videos covers learning in azure general. This includes Databrick, functions, storage 
    https://www.youtube.com/channel/UC_n9wCmDG064tZUKZF2g4Aw

    - Learning the basics of Azure functions 
    Initial 25 videos on using functions within Azure. This is a great place to start to gain a basic understanding of azure functions.
    This will cover setting up azure to work with both visual studio and VS Code.
    https://www.youtube.com/playlist?list=PLMWaZteqtEaLRsSynAsaS_aLzDPBUU4CV

    - Learning the basics Azure Logic Apps, these are a non coding method for creating azure functions
    https://www.youtube.com/watch?v=KxkiE2JC0RU&list=PLMWaZteqtEaIWwpz64BwOBytNDPka700J

    
III. Coding examples

    1. Setting up environment variables. These are called Application settings in Azure. 
    https://www.youtube.com/watch?v=7SYwAjmdFA4
    
    Define the "Local" environment variables
    https://eugenechiang.azurewebsites.net/2021/03/26/azure-function-get-environment-variable-using-local-settings-json/

    When defining an envirnment variable a file called: local.settings.json needs to have these values defined so that they are available
    for local testing.

    - Connection strings are similar to environment variables.
     (These are defined in configuration just under the application setting [Environment variable])
    Note: Connection strings are encrypted at rest and transmitted over an encrypted channel. 
          Connection strings should only be used with a function app if you are using entity framework. 
          For other scenarios use App Settings.

    These are then accessed like an environment variable except you prepend the string with the typ of connection you are using current options are:
    MySQL:      MySQL_<name>
    SQLServer:  SQLServer_<name>
    SQLAzure:   SQLAzure_<name>
    PostgreSQL: PostgreSQL_<name>
    Custom:     Custom_<name>

    2. Use the following site to install SqlClient to make ODBC connection
    https://chanmingman.wordpress.com/2021/12/24/add-system-data-sqlclient-using-visual-studio-code/

    - Basic tutorial to connect to databases: https://www.youtube.com/watch?v=2qW1zsuJ9s0
    Fairly robust solution to connect function to database example of SQL parms and azure environment variable and setting up azure
    https://www.c-sharpcorner.com/article/develop-a-rest-api-with-azure-functions-and-sql/


    3. Using Azure key vault: These are added to an environment variable as: 
    @Microsoft.KeyVault(SecretUri=<Environment variable link>)
    for example:
    @Microsoft.KeyVault(SecretUri=https://osar-dev-vault.vault.azure.net/secrets/rickodb1-database-windows-net-sqluser/c8a3d66cd53d4f0c94b97888746e7c42)
    https://blog.joaograssi.com/using-azure-key-vault-references-with-azure-functions-appservice/#:~:text=On%20the%20Azure%20Portal%2C%20go%20to%20Resource%20groups,to%20your%20Key%20Vault%20when%20the%20deployment%20finishes
    Notes: key vault access costs a little extra money and there is an alternative, see step 4 Connections strings

    Final note on steps 2 - 3, these environment variables are NOT case sensitive. And it seems like Azure uses either all upper case or Camel case,
    so for our variables we will user lower case

    4. Setting SQL ODBC connection for Azure.
    JSON and SQL examples for returning json
    https://docs.microsoft.com/en-us/answers/questions/142700/for-json-outputs-in-multiple-rows.html

    The password

    user:     sqluser
    Password: pswd1234!


    5. Authorization settings for at the function app and function level.
    These keys can be sent via:
    query string: code=<API_KEY>
    HTTP Header: x-functions-key

    AuthorizationLevel object can be set to several levels
    Keys are defined at "App keys" level called "Host keys"
    "master" will work at any level                                 (L1)
    "default" or any other name will work at any function App level (L2)
    Keys defined at the function level "Functio Keys"
    "default" or any name will work at any function App level       (L3)

    Anonymous --> no azure level validation setting. no validation
    Function  --> authourity specified Under App keys levels (L1,L2,L3)
    System    --> authourity specified Under App keys levels (L1,L2)
    Admin     --> authourity specified Under App keys levels (L1)

    Note: you cannot set associate a key to a vault value.

    Site that provides great details on function keys
    https://markheath.net/post/managing-azure-functions-keys-2
    To learn about what keys are accept at each level look at
    https://vincentlauzon.com/2017/12/04/azure-functions-http-authorization-levels/


    6. Techniques for parsing JSON
    For simple JSON files.
    
    JSON multiple rows example
    https://stackoverflow.com/questions/20183395/how-to-parse-multiple-records-in-json

    JSON complex list:
    https://passos.com.au/converting-json-object-into-c-list/

    -Example for linking javascript to website (This is for the future.)
    https://www.c-sharpcorner.com/UploadFile/2ed7ae/jsonresult-type-in-mvc/


    7. link storage account to azure function
    Connection storage account example training
    https://techcommunity.microsoft.com/t5/apps-on-azure-blog/secure-storage-account-linked-to-function-app-with-private/ba-p/2644772

    need to add 
      "AzureWebJobsStorage": "UseDevelopmentStorage=true", to local.settings.json if one wants to use a storage account

      The value "UseDevelopmentStorage=true" converts to:
      "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"
      
      Add to csproj file
      <PackageReference Include="Microsoft.Azure.WebJobs.Extensions.Storage" Version="5.0.0"/> to FirstFunction.csproj


    8. This is setting up a custom security using a environment variable, we can define these keys at the function level or the vault level.
    If defined at the vault level each access will 
    document on key vault pricing.
    https://azure.microsoft.com/en-us/pricing/details/key-vault/
    currently it is $0.03/10,000 transactions

    There is managed key rotation but it is in review

    9. Enabling security at the function App by adding authentication. 
    You can edit the authenticaion to allow both a password and allow unsigned access.


IV. Future settings
    video on securely calling function from website will watch more but may need 
    https://www.youtube.com/watch?v=uST0CyqRIHA

    using OidcAuthentication need to study further
    https://www.youtube.com/watch?v=XaS_p5D1llg 

    C# code to read a key vault
    https://www.codeproject.com/Tips/1430794/Using-Csharp-NET-to-Read-and-Write-from-Azure-Key
*/

using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Primitives; // add FirstOrDefault 

using Microsoft.Azure.WebJobs.Extensions.Storage;
using System.Text;
using System.Threading;
using System.Data;
using System.Data.SqlClient;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Json;

using System.Security.Claims;
namespace My.Function
{
    public static class HttpTrigger1
    {

        [FunctionName("getOptions")]
        public static async Task<IActionResult> getOptions(
            // 5. Authorization settings for at the function app and function level.
            [HttpTrigger(AuthorizationLevel.Function, "options", Route = "options/{test}")] HttpRequest req, 
            string test,
            //[Queue("outputqueue"), StorageAccount("UseDevelopmentStorage")] ICollector<string> msg,
            ILogger log
            )
        {
            log.LogInformation("Get options.");

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            
            StringValues sv;
            req.Headers.TryGetValue("Authorize", out sv);
            var st2 = sv.FirstOrDefault("");
            if (st2 != test)
            {
                log.LogError($"Not Authorized: Header.Authorization != '{test}'");
                return new UnauthorizedResult();
                //return new BadRequestObjectResult($"Not Authorized: Header.Authorization != '{test}'");
            }

            string result = "Access-Control-Allow-Methods: OPTIONS, POST, GET";

            string responseMessage = (result);
            return new OkObjectResult(responseMessage);
        }

        [FunctionName("getName")]
        public static async Task<IActionResult> getName(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = "read")] HttpRequest req, 
            //7. link storage account to azure function
            [Queue("outputqueue"), StorageAccount("AzureWebJobsStorage")] ICollector<string> msg,
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
            
            //It is always a good idea to await results from any process if you want a return value.
            await Task.Run(() => getData(ref result, name, log));

            //1. These are examples of how to interrogate environment variables and connection strings from the azure environment
            //3. These first variable connStr is configured on the Azure side to access the vault
            string connStr = System.Environment.GetEnvironmentVariable("connection_string", EnvironmentVariableTarget.Process);
            string password = System.Environment.GetEnvironmentVariable("CustomCONNSTR_password", EnvironmentVariableTarget.Process);
            string user = System.Environment.GetEnvironmentVariable("CustomCONNSTR_user", EnvironmentVariableTarget.Process);

            string responseMessage = (string.IsNullOrEmpty(result) || result.Trim() == "")
                ? $"This HTTP triggered function executed successfully. However, no data as returned.\nconnStr: {connStr}\npassword: {password}\nuser: {user}"
                : $"Result:\n{result}\nThis HTTP triggered function executed successfully.\nconnStr: {connStr}\npassword: {password}\nuser: {user}";

            //7. link storage account to azure function
            msg.Add($"Result: {result}");

            return new OkObjectResult(responseMessage);
        }

        [FunctionName("GetAllNames")]
        public static async Task<IActionResult> GetAllNames(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = "all")] HttpRequest req, 
            //[Queue("outputqueue"), StorageAccount("UseDevelopmentStorage")] ICollector<string> msg,
            ILogger log
            )
        {
            log.LogInformation("Get ALL Query.");

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();

            ClaimsPrincipal claimIdentity = req.HttpContext.User;
            string user = claimIdentity.Identity.Name;

            string result = "";   

            await Task.Run(() => getData(ref result, string.Empty, log));

            string responseMessage = ($"{user} : {result}");
            return new OkObjectResult(responseMessage);
        }

        [FunctionName("youtubeLinks")]
        public static async Task<IActionResult> PutNames(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "putNames")] HttpRequest req, 
            //[Queue("outputqueue"), StorageAccount("UseDevelopmentStorage")] ICollector<string> msg,
            ILogger log,
            CancellationToken cancellationToken
            )
        {
            log.LogInformation("Put Name Record.");

            int cntr = 0, jsonRows = 0;
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            YoutubeLink[] youtubeLinks = null;

            if (string.IsNullOrEmpty(requestBody) || requestBody == "")
            {
                log.LogWarning("must provide a body to search for.");
                return new BadRequestObjectResult("Expecting the body to contain a value.");
            }

            try
            {
                byte[] bytes = Encoding.UTF8.GetBytes(requestBody);
                using(MemoryStream mStream = new MemoryStream(bytes))
                {
                    DataContractJsonSerializer serializer = new DataContractJsonSerializer(typeof(YoutubeLink[]));
                    youtubeLinks = (YoutubeLink[])serializer.ReadObject(mStream);
                }
                jsonRows = youtubeLinks.Length;
            }
            catch (Exception e)
            {
                log.LogError(e.ToString());
            }

            //It is always a good idea to await results from any process if you want a return value. (We are returning the count...)
            await Task.Run(() => putData(ref cntr, jsonRows, youtubeLinks, log, cancellationToken));

            string responseMessage = ($"{cntr} out of {jsonRows} Record(s)");
            return new OkObjectResult(responseMessage);
        }

        [DataContract]
        public class YoutubeLink
        {      
             // for newtonsoft use JsonProperty
             //[JsonProperty("namester")] 
             [DataMember(Name = "namester")]
            public string name { get; set; }
            [DataMember]
            public string url { get; set; }
        }

        // 4. Setting SQL ODBC connection for Azure.
        public static string connect()
        {
            string connStr = System.Environment.GetEnvironmentVariable("connection_string", EnvironmentVariableTarget.Process);
            string password = System.Environment.GetEnvironmentVariable("CustomCONNSTR_password", EnvironmentVariableTarget.Process);
            string user = System.Environment.GetEnvironmentVariable("CustomCONNSTR_user", EnvironmentVariableTarget.Process);

            return connStr.Replace("{user}", user).Replace("{password}", password);
        }
        public static void putData(ref int cnt, int jsonRows, YoutubeLink[] ytls, ILogger log, CancellationToken cancellationToken)
        {
            string query = "INSERT INTO YoutubeLinks(name, url) VALUES(@Name, @Url)";
            cnt = 0;
            try
            {
                using (SqlConnection connection = new SqlConnection(connect()))
                {
                    connection.Open();
                    foreach (YoutubeLink ytl in ytls)
                    {
                        if (cancellationToken.IsCancellationRequested)
                        {
                            log.LogInformation("A cancellation token was received.");
                            return;
                        }
                        try
                        {
                            SqlCommand command = new SqlCommand(query, connection);
                            command.Parameters.AddWithValue("@Name", ytl.name);
                            command.Parameters.AddWithValue("@Url", ytl.url);
                            command.ExecuteNonQuery();
                            cnt++;
                        }
                        catch (Exception e)
                        {
                            log.LogError(e.ToString());
                        }
                    }
                }
            }
            catch (Exception e)
            {
                log.LogError(e.ToString());
            }       
        }

        public static void getData(ref string result, string name, ILogger log)
        {           
            string query;
            
            if (name == string.Empty)
            {
                query = "SELECT * FROM dbo.YoutubeLinks FOR JSON AUTO, ROOT('All')";
            }
            else
            {
                query = "SELECT * FROM dbo.YoutubeLinks WHERE name=@Name FOR JSON AUTO; --, Without_Array_Wrapper";
            }

            DataTable dt = new DataTable();
            try
            {
                using (SqlConnection connection = new SqlConnection(connect()))
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
