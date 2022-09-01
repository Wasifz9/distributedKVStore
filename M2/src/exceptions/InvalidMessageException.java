package exceptions;

public class InvalidMessageException extends Exception
{
      // Parameterless Constructor
      public InvalidMessageException() {}

      // Constructor that accepts a message
      public InvalidMessageException(String errorMessage)
      {
         super(errorMessage);
      }

      // Constructor that accepts a message and throwable
      public InvalidMessageException(String errorMessage, Throwable err) {
        super(errorMessage, err);
    }
 }
