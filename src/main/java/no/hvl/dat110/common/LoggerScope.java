package no.hvl.dat110.common;

@FunctionalInterface
public interface LoggerScope extends AutoCloseable{
    @Override
    void close();
}