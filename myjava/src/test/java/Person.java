import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

import java.nio.charset.Charset;

/**
 * @Version 1.0
 * @Author ZHANGBAIFA
 * @Date 2021/5/13 20:38
 * @Description:
 */
public class Person {
    private String firstName;
    private String lastName;

    public Person(String firstName, String lastName) {
        this.firstName = firstName;
        this.lastName = lastName;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }
}
enum PersonFunnel implements Funnel<Person> {
    INSTANCE;

    @Override
    public void funnel(Person person, PrimitiveSink into) {
        into.putString(person.getFirstName(), Charset.defaultCharset())
                .putString(person.getLastName(), Charset.defaultCharset());
    }
}