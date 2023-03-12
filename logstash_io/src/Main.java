import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Scanner;

/**
 * @Author: SayHello
 * @Date: 2023/3/12 10:25
 * @Introduction:
 */
public class Main {
    public static void main(String[] args) throws IOException {
        // 1.定义服务器的地址、端口号、数据
        InetAddress address = InetAddress.getByName("127.0.0.1");
        // 2.定义服务器端口
        int port = 1234;

        // 3.创建发送端对象：发送端自带默认端口号
        DatagramSocket socket = new DatagramSocket(2222);

        // 4.客户端启动成功，输出提示信息
        System.out.println("****客户端启动成功****");

        // 5.向服务端发送信息
        new Thread(() -> {
            try {
                while (true) {
                    System.out.println("请输入：");
                    // 5.1 从键盘接受数据
                    Scanner sc = new Scanner(System.in);
                    // 5.2 nextLine方式接受字符串
                    String msg = sc.nextLine();
                    // 5.3 创建一个数据包对象封装数据
                    byte[] buffer = msg.getBytes();
                    DatagramPacket packets = new DatagramPacket(buffer, buffer.length, address, port);
                    // 5.4 发送数据出去
                    socket.send(packets);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();


    }
}
