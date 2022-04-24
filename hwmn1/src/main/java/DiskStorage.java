import ru.mipt1c.homework.task1.KeyValueStorage;
import ru.mipt1c.homework.task1.MalformedDataException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;

import static java.nio.file.StandardOpenOption.*;

public class DiskStorage<K, V> implements KeyValueStorage<K, V>{
    Path filePath;
    HashMap <K, V> inMemoryHashMap;
    int newChanges;
    private final int modificationsBufferLimit = 3;
    InputStream inputStream;
    OutputStream outputStream;
    ObjectOutputStream objWriter;
    ObjectInputStream objReader;
    public DiskStorage(String directoryPath) throws MalformedDataException{
        Path path = Paths.get(directoryPath);
        boolean directoryExists = Files.exists(path) && Files.isDirectory(path);
        if(!directoryExists)
            throw new MalformedDataException("directory does not exist :(");
        filePath = Paths.get(directoryPath, "hashmapdump.bin");
        boolean existed = Files.exists(filePath);
        if(!existed){
            try {
                Files.createFile(filePath);
            }
            catch (IOException exception){
                throw new MalformedDataException(exception);
            }
            inMemoryHashMap = new HashMap <K, V>();
        }
        try {
            outputStream = Files.newOutputStream(filePath, WRITE);
            objWriter = new ObjectOutputStream(outputStream);
        }
        catch (IOException exception) {
            throw new MalformedDataException(exception);
        }
        if(existed){
            try {
                inputStream = Files.newInputStream(filePath, READ);
                objReader = new ObjectInputStream(inputStream);
                inMemoryHashMap = (HashMap<K, V>) objReader.readObject();
                objReader.close();
                inputStream.close();
            }
            catch (ClassNotFoundException | IOException exception) {
                throw new MalformedDataException(exception);
            }
        }
        newChanges = 0;
    }

    @Override
    public V read(K key) {
        if(exists(key)){
            return inMemoryHashMap.get(key);
        }
        return null;
    }

    @Override
    public boolean exists(K key) {
        return inMemoryHashMap.containsKey(key);
    }

    @Override
    public void write(K key, V value) {
        inMemoryHashMap.put(key, value);
        newChanges++;
        if(newChanges > modificationsBufferLimit){
            flush();
        }
    }

    @Override
    public void delete(K key) {
        inMemoryHashMap.remove(key);
        newChanges++;
        if(newChanges > modificationsBufferLimit){
            flush();
        }
    }

    @Override
    public Iterator<K> readKeys() {
        return inMemoryHashMap.keySet().iterator();
    }

    @Override
    public int size() {
        return inMemoryHashMap.size();
    }

    @Override
    public void flush() {
        try {

            objWriter.writeObject(inMemoryHashMap);
            objWriter.flush();
            newChanges = 0;
        }
        catch (IOException exception) {
            throw new MalformedDataException(exception);
        }
    }

    @Override
    public void close() throws IOException {
        flush();
        objWriter.close();
        outputStream.close();
    }
}
