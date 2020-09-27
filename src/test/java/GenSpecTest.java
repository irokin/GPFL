import org.junit.Test;
import uk.ac.ncl.model.GenSpec;

import java.io.File;

public class GenSpecTest {

    @Test
    public void runTest() {
        File config = new File("data/WN18RR/config.json");
        GenSpec system = new GenSpec(config);
        system.run();
    }

    @Test
    public void learnTest() {
        File config = new File("data/UWCSE/config.json");
        GenSpec system = new GenSpec(config);
        system.learn();
    }

    @Test
    public void applyTest() {
        File config = new File("data/WN18RR/config.json");
        GenSpec system = new GenSpec(config);
        system.apply();
    }

    @Test
    public void scoreTest() {
        File config = new File("data/UWCSE/gs_config.json");
        GenSpec system = new GenSpec(config);
        system.score();
    }

    @Test
    public void inMemoryGraphTest() {
        File config = new File("data/UWCSE/config.json");
        GenSpec system = new GenSpec(config);
        system.buildGraph();
    }
}

