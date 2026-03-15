package no.hvl.dat110.alotofnewstuff;

@FunctionalInterface
public interface Interruptable {
    void run() throws InterruptedException;
}