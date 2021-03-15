package com.pro100kryto.server.services.usersdatabasemongo;

import com.pro100kryto.server.services.usersdatabase.connection.Consts;
import com.pro100kryto.server.services.usersdatabase.connection.UserDB;
import org.bson.Document;

import java.util.Objects;

public class UserDBImpl extends UserDB {
    private final Document document;

    public UserDBImpl(Document document){
        super(document.getLong(Consts.USER_ID));
        this.document = document;
    }

    public UserDBImpl(long userId, Document document) {
        super(userId);
        Objects.requireNonNull(document);
        this.document = document;
    }

    @Override
    public Object getValue(String key) {
        return document.get(key);
    }
}
