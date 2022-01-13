package edu.uci.ics.texera.web.resource;

import edu.uci.ics.texera.web.ServletAwareConfigurator;

import javax.websocket.*;
import javax.websocket.server.ServerEndpoint;
import java.util.HashMap;


@ServerEndpoint(
        value = "/collaboration",
        configurator = ServletAwareConfigurator.class
)
public class CollaborationResource {
    public static HashMap<String, Session> websocketSessionMap = new HashMap<>();

    @OnOpen
    public void myOnOpen(final Session session) {
        websocketSessionMap.put(session.getId(), session);
    }

    @OnMessage
    public void myOnMsg(final Session session, String message) {

        message = "{" + message.substring(1, message.length() - 1) + "}";

        for(String key: websocketSessionMap.keySet()) {
            // only send to other sessions, not the session that send the message
            Session sess = websocketSessionMap.get(key);
                if (sess != session) {
                    websocketSessionMap.get(key).getAsyncRemote().sendText(message);
                }
            System.out.println("Message propagated: " + message);
        }
    }

    @OnClose
    public void myOnClose(final Session session, CloseReason cr) {
        websocketSessionMap.remove(session.getId());
        System.out.println("Session disconnected");
    }
}