package lab7;
import org.zeromq.*;
import java.util.ArrayList;
//Центральный прокси.
public class Proxy {
    private static ZMQ.Poller poll;
    private static ZContext context;
    private static ZMQ.Socket sClient;
    private static ZMQ.Socket sStorage;
    private static ArrayList<Cache> caches;

    public static void main(String[] args) {
        context = new ZContext();
        caches = new ArrayList<>();
        //Открывает два сокета ROUTER.
        sClient = context.createSocket(SocketType.ROUTER);
        sStorage = context.createSocket(SocketType.ROUTER);
        sClient.bind("tcp://localhost:8001");
        sStorage.bind("tcp://localhost:8002");
        System.out.println("Start");
        poll = context.createPoller(2);
        //От одного принимаются команды от клиентов.
        poll.register(sClient, ZMQ.Poller.POLLIN);
        //От другого принимаются - команды NOTIFY.
        poll.register(sStorage, ZMQ.Poller.POLLIN);
        while (poll.poll(3000) != -1) {
            //Сообщения от клиента имеют индекс 0
            if (poll.pollin(0)){
                //Команда от клиентов содержит номер ячейки и тип (PUT или GET)
                //При получении команды от клиента ищем в актуальном списке частей кэша —
                //подходящие (интервал которых содержит заданный номер ячейки) и рассылаем
                //всем им команду PUT.
                ZMsg recv = ZMsg.recvMsg(sClient);
                String msg = new String(recv.getLast().getData(), ZMQ.CHARSET);
                String[] msgSplit = msg.split(" ");
                String command = msgSplit[0];
                System.out.println(msgSplit[0] + " " + msgSplit[1]);
                if (command.equals("GET")){
                    int key = Integer.parseInt(msgSplit[1]);
                    sendG(key, recv);
                    //Команда GET отправляется случайному серверу содержащему ячейку.
                } else if (command.equals("PUT")){
                    int key = Integer.parseInt(msgSplit[1]);
                    sendP(key, recv);
                }
                //С помощью команд NOTIFY ведется актуальный список подключенных частей кэша.
            //Собщения от хранилища имеют индекс 1
            } else if (poll.pollin(1)){
                //Получаем сообщение из Хранилища
                ZMsg recv = ZMsg.recvMsg(sStorage);
                ZFrame frame = recv.unwrap();
                String id = new String(frame.getData(), ZMQ.CHARSET);
                String msg = new String(recv.getFirst().getData(), ZMQ.CHARSET);
                String[] msgSplit = msg.split(" ");
                String command = msgSplit[0];
                if (command.equals("INIT")) {
                    int end = Integer.parseInt(msgSplit[2]);
                    int start = Integer.parseInt(msgSplit[1]);
                    caches.add(new Cache(frame, id, System.currentTimeMillis(), start, end));
                }else if (command.equals("TIMEOUT")){
                    changeTimeout(id);
                } else {recv.send(sClient);}
            }
            System.out.println("Proxy loop...");
        }
        //Все закрываем и уничтожаем
        context.destroySocket(sClient);
        context.destroySocket(sStorage);
        context.destroy();
    }

    private static void changeTimeout(String id) {
        for (Cache cache : caches) {
            if (cache.checkID(id)) {
                cache.setTimeout(System.currentTimeMillis());
            }
        }
    }

    private static void sendG(int key, ZMsg recv) {
        //Ставим флаг
        boolean flag = false;
        for (Cache cache : caches) {
            if (cache.getStart()  <= key && cache.getEnd() >= key){
                cache.getFrame().send(sStorage, ZFrame.REUSE + ZFrame.MORE);
                recv.send(sStorage, false);
                flag = true;
            }
        }
        //Если флаг не поменялся
        if (!flag) {
            recv.getLast().reset("er");
            recv.send(sClient);
        }
    }

    private static void sendP(int key, ZMsg recv) {
        //Ставим счетчик
        int count = 0;
        for (Cache cache : caches) {
            if (cache.getStart() <= key && cache.getEnd() >= key){
                cache.getFrame().send(sStorage, ZFrame.REUSE + ZFrame.MORE);
                recv.send(sStorage, false);
                count++;
            }
        }
        ZMsg response = new ZMsg();
        response.add(new ZFrame("put to " + count));
        response.wrap(recv.getFirst());
        response.send(sClient);
    }
}
