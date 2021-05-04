import org.apache.commons.lang.StringEscapeUtils;

/**
 * @Version 1.0
 * @Author ZHANGBAIFA
 * @Date 2021/4/29 10:55
 * @Description:
 */
public class Test {
    public static void main(String[] args) {

        String str = "thi is a test 这是一个测试";
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                + "<persons>\n" + " <person id=\"23\">\n"
                + " <name>张 三</name>\n" + " <age>26</age>\n" + "</person>\n"
                + " <person id=\"22\">\n" + " <name>李四</name>\n"
                + " <age>25</age>\n" + " </person>\n" + "</persons>";
        System.out.println("用escapeJava方法转义之后的字符串为:"
                + StringEscapeUtils.escapeJava(str));
        System.out.println(StringEscapeUtils.unescapeJava(str));
    }
}



