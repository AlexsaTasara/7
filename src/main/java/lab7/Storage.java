package lab7;
import org.zeromq.*;
import java.util.Map;
import java.util.Scanner;
import java.util.HashMap;

//Хранилище части распределенного кэша.
public class Storage {
    private static long timeout;
    private static ZContext context;
    private static ZMQ.Poller poller;
    private static Map<Integer, String> storage;

    public static void main(String[] args) {
        context = new ZContext();
        //Сокет - точка подключения одной системы к другой.
        //Открывает сокет DEALER, подключается к центральному прокси.
        ZMQ.Socket socket = context.createSocket(SocketType.DEALER);
        socket.connect("tcp://localhost:8002");
        storage = new HashMap<>();
        Scanner in = new Scanner(System.in);
        int start = in.nextInt();
        int end = in.nextInt();
        ZFrame initFrame = new ZFrame("INIT" + " " + start + " " + end);
        initFrame.send(socket, 0);
        System.out.println("Storage start on tcp://localhost:8002");
        poller = context.createPoller(1);
        poller.register(socket, ZMQ.Poller.POLLIN);
        timeout = System.currentTimeMillis() + 3000;
        while (poller.poll(3000) != -1){
            //После подключения с определнным интервалом времени высылает сообщение NOTIFY в котором сообщает
            //интервал хранимых значений.
            isTimeout(socket);
            if (poller.pollin(0)){
                ZMsg recv = ZMsg.recvMsg(socket);
                if (recv.size() == 3) {
                    //Также принимает из сокета два вида команд
                    String[] message = recv.getLast().toString().split(" ");
                    String command = message[0];
                    //На изменение ячейки кэша.
                    if (command.equals("GET")){
                        int key = Integer.parseInt(message[1]);
                        sendG(key, recv, socket);
                        //На извлечение ячейки.
                    } else if (command.equals("PUT")){
                        int key = Integer.parseInt(message[1]);
                        String val = message[2];
                        sendP(key, val, recv);
                    }
                }
            }
        }
        context.destroySocket(socket);
        context.destroy();
    }

    private static void isTimeout(ZMQ.Socket socket) {
        if (System.currentTimeMillis() >= timeout) {
            System.out.println("TIMEOUT");
            timeout = System.currentTimeMillis() + 3000;
            ZFrame frame = new ZFrame("TIMEOUT");
            frame.send(socket, 0);
        }
    }

    private static void sendP(int key, String val, ZMsg recv) {
        storage.put(key, val);
        recv.destroy();
        System.out.println("PUT | key: " + key + " | val: " + val);
    }

    private static void sendG(int key, ZMsg recv, ZMQ.Socket socket) {
        String answer = storage.getOrDefault(key, "ex");
        recv.getLast().reset(answer);
        recv.send(socket);
        System.out.println("GET | key: " + key);
    }
}