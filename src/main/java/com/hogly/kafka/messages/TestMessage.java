package com.hogly.kafka.messages;

public class TestMessage<T> {

  private Integer quantity;
  private T payload;

  TestMessage(Integer quantity, T payload) {
    this.quantity = quantity;
    this.payload = payload;
  }

  public static <T> TestMessage of(Integer quantity, T payload) {
    return new TestMessage<T>(quantity, payload);
  }

  public Integer quantity() {
    return quantity;
  }

  public T payload() {
    return payload;
  }

  @Override
  public String toString() {
    return "TestMessage{" +
      "quantity=" + quantity +
      ", payload=" + payload +
      '}';
  }
}
