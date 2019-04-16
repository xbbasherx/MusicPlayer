import java.rmi.*;

import DFS.PagesJson;

import java.io.*;

public interface ChordMessageInterface extends Remote
{
    public ChordMessageInterface getPredecessor()  throws RemoteException;
    ChordMessageInterface locateSuccessor(long key) throws RemoteException;
    ChordMessageInterface closestPrecedingNode(long key) throws RemoteException;
    public void joinRing(String Ip, int port)  throws RemoteException;
    public void joinRing(ChordMessageInterface successor)  throws RemoteException;
    public void notify(ChordMessageInterface j) throws RemoteException;
    public boolean isAlive() throws RemoteException;
    public long getId() throws RemoteException;
    
    
    public void put(long guidObject, RemoteInputFileStream inputStream) throws IOException, RemoteException;
    public void put(long guidObject, String text) throws IOException, RemoteException;
    public RemoteInputFileStream get(long guidObject) throws IOException, RemoteException;   
    public byte[] get(long guidObject, long offset, int len) throws IOException, RemoteException;  
    public void delete(long guidObject) throws IOException, RemoteException;
    
    public void onChordSize(long source, int n);
    public void onPageCompleted(File file);
    public void mapContext(CatalogPage page, Mapper mapper, ChordMessageInterface coordinator, File file);
    public void reduceContext(CatalogPage page, Mapper reducer, ChordMessageInterface coordinator, File file);
    public void addKeyValue(long key, int value);
    public void emit(long key, int value, File file);
    public <PagesJson> void bulk(PagesJson page);
    
}
