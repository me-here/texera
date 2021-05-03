/*
 * This file is generated by jOOQ.
 */
package edu.uci.ics.texera.web.model.jooq.generated.tables.pojos;


import edu.uci.ics.texera.web.model.jooq.generated.tables.interfaces.IUser;

import org.jooq.types.UInteger;


/**
 * This class is generated by jOOQ.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class User implements IUser {

    private static final long serialVersionUID = -644617017;

    private String   name;
    private UInteger uid;
    private String   password;

    public User() {}

    public User(IUser value) {
        this.name = value.getName();
        this.uid = value.getUid();
        this.password = value.getPassword();
    }

    public User(
        String   name,
        UInteger uid,
        String   password
    ) {
        this.name = name;
        this.uid = uid;
        this.password = password;
    }

    /**
     * Getter for <code>texera_db.user.name</code>.
     */
    @Override
    public String getName() {
        return this.name;
    }

    /**
     * Setter for <code>texera_db.user.name</code>.
     */
    @Override
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Getter for <code>texera_db.user.uid</code>.
     */
    @Override
    public UInteger getUid() {
        return this.uid;
    }

    /**
     * Setter for <code>texera_db.user.uid</code>.
     */
    @Override
    public void setUid(UInteger uid) {
        this.uid = uid;
    }

    @Override
    public String getPassword() {
        return this.password;
    }

    @Override
    public void setPassword(String password) {
        this.password = password;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("User (");

        sb.append(name);
        sb.append(", ").append(uid);
        sb.append(", ").append(password);

        sb.append(")");
        return sb.toString();
    }

    // -------------------------------------------------------------------------
    // FROM and INTO
    // -------------------------------------------------------------------------

    @Override
    public void from(IUser from) {
        setName(from.getName());
        setUid(from.getUid());
        setPassword(from.getPassword());
    }

    @Override
    public <E extends IUser> E into(E into) {
        into.from(this);
        return into;
    }
}
