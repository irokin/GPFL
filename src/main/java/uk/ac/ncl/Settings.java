package uk.ac.ncl;

public class Settings {

    // Application Info
    public static String VERSION = "public-0.1";
    public static String DATE = "May-2020";

    // GenSpec Settings
    public static int COVER_REPEATS = 3000;
    public static int TOP_RULES = 1000000;

    // Global Booleans
    public static boolean PRIOR_FILTERING = true;
    public static boolean POST_FILTERING = false;

    /**
     * Max number of instantiated rules allowed to be learned for a target.
     */
    public static int INS_RULE_CAP = 15000000;

    /**
     * In rule application, the max number of predictions allowed
     * for a type of instantiated rule.
     */
    public static int SUGGESTION_CAP = 15000000;

    public static int PREDICTION_RULE_CAP = 20;

    /**
     * Max time on specialization
     */
    public static int SPEC_TIME = Integer.MAX_VALUE;

    /**
     * Max time on generating essential rules.
     */
    public static int ESSENTIAL_TIME = Integer.MAX_VALUE;

    /**
     * Max time on generalization.
     */
    public static int GEN_TIME = Integer.MAX_VALUE;
    public static int RANDOM_WALKERS = 10;
    public static boolean USE_SIGMOID = false;
    public static double OVERFITTING_FACTOR = 0;

    //###############End#################

    public static boolean ALLOW_INS_REVERSE = false;
    public static double VALID_PRECISION = 0;
    public static double HIGH_QUALITY = 1;

    /**
     * We implement three types of protocol:
     * - TransE: Summing over testing triples
     * - GPFL: Taking the average of the score over all relation types.
     * - All: Print results of all protocols.
     */
    public static String EVAL_PROTOCOL = "TransE";

    public static int MAX_RECURSION_DEPTH = 1000;

    /**
     *  Select which rule quality measure to use from:
     *  smoothedConf
     *  standardConf
     *  pcaConf
     *  apcaConf
     */
    public static String QUALITY_MEASURE = "smoothedConf";

    /**
     * The abstract rule saturation.
     */
    public static double SATURATION = 0.99;

    /**
     * The max number of groundings for evaluating abstract rules.
     * when = 0, the system finds all groundings of rules.
     */
    public static int LEARN_GROUNDINGS = 100000;

    /**
     * The max number of groundings for suggesting predicted facts.
     * When = 0, the system finds all groundings of rules.
     */
    public static int APPLY_GROUNDINGS = Integer.MAX_VALUE;

    /**
     * Standard confidence threshold.
     */
    public static double CONF = 0.0001;

    /**
     * Head coverage threshold for rule pruning. Alternative to Support threshold.
     */
    public static double HEAD_COVERAGE = 0;

    /**
     * Support threshold for rule pruning.
     */
    public static int SUPPORT = 2;

    /**
     * Laplace smoothing to make rules with small total predictions but high correct
     * predictions less competitive.
     */
    public static int CONFIDENCE_OFFSET = 5;

    /**
     * The max depth of learned rules.
     */
    public static int DEPTH = 3;
    public static int CAR_DEPTH = 3;
    public static int INS_DEPTH = 3;

    /**
     * The number of top-ranked predicted facts that will be written to prediction file for each query.
     */
    public static int TOP_K = 10;

    /**
     * Thread number for multi-threading works.
     */
    public static int THREAD_NUMBER = 4;

    /**
     * Logging and debugging print priority.
     * = 1, print only timer and memory usage
     * = 2, print greetings
     * = 3, print debugging infos
     */
    public static int VERBOSITY = 2;

    /**
     * If a target relation has less instances than this threshold,
     * the system will ignore it.
     */
    public static int MIN_INSTANCES = 0;
    public static int MAX_INSTANCES = Integer.MAX_VALUE;

    /**
     * The path batch size needed between abstract rule saturation check.
     */
    public static int BATCH_SIZE = 20000;

    /**
     * Specify how many relationship types one wants to learn rules for.
     * When = 0, the system will learn rules for all of the types discovered in the knowledge graph.
     */
    public static int RANDOMLY_SELECTED_RELATIONS = 0;

    /**
     * Specify the max number of top-ranked open abstract rules.
     * Only the selected open abstract rules will be evaluated and used to
     * generated instantiated rules. Note that this will not affect closed abstract rules.
     * When = 0, the system will use all of the rules.
     */
    public static int TOP_ABS_RULES = 500;

    /**
     * The identifier used in the Neo4J database for uniquely defining an entity.
     */
    public static String NEO4J_IDENTIFIER = "name";

    /**
     * The max number OF rules for each prediction in the verification file.
     */
    public static int VERIFY_RULE_SIZE = 5;

    /**
     * The max number of predictions for each query in the verification file.
     */
    public static int VERIFY_PREDICTION_SIZE = 3;

    /**
     * If create a Rule Mapping Graph from verification data.
     */
    public static boolean RULE_GRAPH = false;

    /**
     * Legacy setting. Now serve as a static variable storing current learning target.
     */
    public static String TARGET = null;

    public static boolean TARGET_FUNCTIONAL = false;

    //###############End#################
}
