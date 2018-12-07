package docs.home.persistence;

import java.util.Objects;

public final class UserGreeting {
  private final String user;
  private final String message;

  public UserGreeting(String user, String message) {
    this.user = user;
    this.message = message;
  }

  public String getUser() {
    return user;
  }

  public String getMessage() {
    return message;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    UserGreeting that = (UserGreeting) o;
    return Objects.equals(user, that.user) &&
        Objects.equals(message, that.message);
  }

  @Override
  public int hashCode() {
    return Objects.hash(user, message);
  }
}
