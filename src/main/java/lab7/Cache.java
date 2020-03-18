package lab7;
import org.zeromq.ZFrame;

public class Cache{
    private int end;
    private int start;
    private String id;
    private ZFrame frame;
    private long timeout;
    //Сохраняем значения
    public Cache(ZFrame frame, String id, long timeout, int start, int end){
        this.id = id;
        this.end = end;
        this.start = start;
        this.frame = frame;
        this.timeout = timeout;
    }
    //Возвращают конец
    public int getEnd() {
        return end;
    }
    //Возвращаем начало
    public int getStart() {
        return start;
    }
    //Возвращаем фрейм
    public ZFrame getFrame() {
        return frame;
    }
    //Проверяем ID, если имеющийся ID = проверяемому, выдаем true
    public boolean checkID(String id) {
        return this.id.equals(id);
    }
    //Устанавливаем время в миллесекундах
    public void setTimeout(long currentTimeMillis) {
        this.timeout = currentTimeMillis;
    }
}
