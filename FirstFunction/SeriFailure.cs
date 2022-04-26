using System.Text;

// This is an example of dependency injection with a variable that can then be interrogated
// by a calling function. This example utilizes the serrilogger to catch message where
// Serilog fails to exeute. This is just an example since the Serilogger will reside in a 
// separate function.
namespace SeriFailure
{
    public interface ISeriFailed
    {
        StringBuilder Messages();
    }

    public class SeriFailed : ISeriFailed
    {
        StringBuilder _messages;

        public SeriFailed(StringBuilder messages)
        {
            _messages = messages;
        }

        public StringBuilder Messages()
        {
            return _messages;
        }
    }
}