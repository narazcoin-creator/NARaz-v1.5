package com.naraz.app;

import android.os.Handler;
import android.os.Looper;
import android.webkit.WebView;

import org.json.JSONObject;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

public class NetworkDiscovery {
    private static final int PORT = 39555;
    private volatile boolean running = false;
    private Thread thread;
    private final WebView web;

    public NetworkDiscovery(WebView web) { this.web = web; }

    public void start() {
        if (running) return;
        running = true;
        thread = new Thread(() -> {
            try (DatagramSocket socket = new DatagramSocket(PORT, InetAddress.getByName("0.0.0.0"))) {
                socket.setBroadcast(true);
                socket.setSoTimeout(1500);
                byte[] buf = new byte[8192];
                while (running) {
                    try {
                        DatagramPacket packet = new DatagramPacket(buf, buf.length);
                        socket.receive(packet);
                        String raw = new String(packet.getData(), packet.getOffset(), packet.getLength());
                        JSONObject o = new JSONObject(raw);
                        if (!"NARAZ_NETWORK_V1".equals(o.optString("magic"))) continue;
                        String ip = packet.getAddress().getHostAddress();
                        int httpPort = o.optInt("http_port", 8080);
                        int wsPort = o.optInt("ws_port", httpPort + 1);
                        String http = "http://" + ip + ":" + httpPort;
                        String ws = "ws://" + ip + ":" + wsPort;
                        new Handler(Looper.getMainLooper()).post(() -> web.evaluateJavascript(
                                "window.setDiscoveredNetwork && window.setDiscoveredNetwork(" +
                                JSONObject.quote(http) + "," + JSONObject.quote(ws) + ");", null));
                    } catch (Exception ignored) {}
                }
            } catch (Exception ignored) {}
        }, "naraz-lan-discovery");
        thread.setDaemon(true);
        thread.start();
    }

    public void stop() {
        running = false;
        if (thread != null) thread.interrupt();
    }
}
