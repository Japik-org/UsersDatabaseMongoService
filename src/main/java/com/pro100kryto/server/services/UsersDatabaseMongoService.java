package com.pro100kryto.server.services;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.result.InsertOneResult;
import com.pro100kryto.server.module.ModuleConnectionSafe;
import com.pro100kryto.server.modules.databasemongo.connection.IDatabaseMongoModuleConnection;
import com.pro100kryto.server.service.AServiceType;
import com.pro100kryto.server.service.Service;
import com.pro100kryto.server.services.usersdatabase.connection.Consts;
import com.pro100kryto.server.services.usersdatabase.connection.IUsersDatabaseServiceConnection;
import com.pro100kryto.server.services.usersdatabase.connection.UserDB;
import com.pro100kryto.server.services.usersdatabasemongo.UserDBImpl;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

public class UsersDatabaseMongoService extends AServiceType<IUsersDatabaseServiceConnection> {
    private ModuleConnectionSafe<IDatabaseMongoModuleConnection> databaseConnectionSafe;
    private String collectionName;

    public UsersDatabaseMongoService(Service service) {
        super(service);

    }

    @Override
    protected IUsersDatabaseServiceConnection createServiceConnection() throws Throwable {
        /*
        String portStr = settings.getOrDefault("rmi-port", "");

        if (portStr.isEmpty()) {
            return new UsersDatabaseServiceConnection();

        } else {
            return new UsersDatabaseServiceConnection(
                    Integer.parseInt(portStr)
            );
        }
         */
        return new UsersDatabaseServiceConnection();
    }

    @Override
    protected void beforeStart() throws Throwable {
        databaseConnectionSafe = new ModuleConnectionSafe<>(service,
                callback.getSettingOrDefault("databasemongo-module-name", "databaseMongo"));
        collectionName = callback.getSettingOrDefault("collection-name", "users");

        try {
            if (!databaseConnectionSafe.getModuleConnection().existCollection(collectionName))
                databaseConnectionSafe.getModuleConnection().createCollection(collectionName);
        } catch (Throwable ignored){
        }
    }

    private final class UsersDatabaseServiceConnection
            implements IUsersDatabaseServiceConnection{

        public UsersDatabaseServiceConnection() {
        }

        @Override
        public boolean ping() {
            return true;
        }

        @Override
        public boolean userExistsById(long userId) {
            return databaseConnectionSafe.getModuleConnection()
                    .countDocuments(
                            collectionName,
                            new Document().append(Consts.USER_ID, userId)
                    )!=0;
        }

        @Override
        public boolean userExistsByKey(String key, Object value) {
            return databaseConnectionSafe.getModuleConnection()
                    .countDocuments(
                            collectionName,
                            new Document().append(key, value)
                    )!=0;
        }

        @Override
        public long createUser(String name, Map<String, Object> keyValMap) {
            Document userDoc = new Document();
            ObjectId objectId = new ObjectId();
            userDoc.append("_id", objectId);
            userDoc.append(Consts.USER_ID, ByteBuffer.wrap(Arrays.copyOfRange(objectId.toByteArray(),
                    4, 12)).getLong());

            InsertOneResult result = databaseConnectionSafe.getModuleConnection()
                    .insertDocument(collectionName, userDoc);

            if (!result.wasAcknowledged()) return -1;

            return userDoc.getLong(Consts.USER_ID);
        }

        @Override @Nullable
        public UserDB getUser(long userId) {
            Document userDoc = databaseConnectionSafe.getModuleConnection()
                    .findFirstDocument(
                            collectionName,
                            new Document()
                                    .append(Consts.USER_ID, userId));

            try {
                return new UserDBImpl(userId, userDoc);
            } catch (NullPointerException ignored){
            }
            return null;
        }

        @Override @Nullable
        public UserDB getFirstUser(String key, Object value) {
            Document userDoc = databaseConnectionSafe.getModuleConnection()
                    .findFirstDocument(
                            collectionName,
                            new Document()
                                    .append(key, value));

            try {
                return new UserDBImpl(userDoc);
            } catch (NullPointerException ignored){
            }
            return null;
        }

        @Override
        public Iterable<UserDB> getAllUsers(String key, Object value) throws RemoteException {
            FindIterable<Document> userDoc = databaseConnectionSafe.getModuleConnection()
                    .findDocuments(
                            collectionName,
                            new Document()
                                    .append(key, value));

            return new Iterable<UserDB>() {
                final MongoCursor<Document> cursor = userDoc.cursor();
                @Override
                public Iterator<UserDB> iterator() {
                    return new Iterator<UserDB>() {
                        @Override
                        public boolean hasNext() {
                            return cursor.hasNext();
                        }

                        @Override
                        public UserDB next() {
                            Document doc = cursor.next();
                            return new UserDBImpl(doc);
                        }
                    };
                }
            };
        }

        @Override @Nullable
        public Object getFirstUserVal(long userId, String key) {
            Document userDoc = databaseConnectionSafe.getModuleConnection()
                    .findFirstDocument(
                            collectionName,
                            new Document()
                                    .append(Consts.USER_ID, userId));

            try{
                return userDoc.get(key);
            } catch (NullPointerException ignored){
            }
            return null;
        }

        @Override @Nullable
        public Object getFirstUserVal(String key, Object value, String key2) {
            Document userDoc = databaseConnectionSafe.getModuleConnection()
                    .findFirstDocument(
                            collectionName,
                            new Document()
                                    .append(key, value));

            try{
                return userDoc.get(key);
            } catch (NullPointerException ignored){
            }
            return null;
        }
    }
}
