package testing.helpers;

import shared.messages.IKVMessage;

public interface IResponseRunnable extends Runnable {
    public IKVMessage getResponse();
    public int getID();
}