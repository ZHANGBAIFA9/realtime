import java.util.ArrayList;

/**
 * @Version 1.0
 * @Author ZHANGBAIFA
 * @Date 2021/4/27 10:48
 * @Description:
 */
public class Test {
    public static void main(String[] args) {
        ArrayList<Person> personArrayList = new ArrayList<>();
        Person person = new Person();
        for(int i = 0 ; i < 10 ;i++){
            person.setName("zhangsan");
            person.setAge(2);
            personArrayList.add(person);
        }
        System.out.println(personArrayList.get(0)+""+personArrayList.get(2));
    }
}
