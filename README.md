## GPFL - Graph Path Feature Learning

### Introduction
GPFL is a probabilistic logical rule learner optimized to mine instantiated first-order logic rules that contain constants from knowledge graphs.

Features:
- Significantly faster than existing methods at mining instantiated rules
- Embedded validation method filtering out overfitting rules
- Fully implemented on Neo4j graph database
- Adaptive to machines of diverse specifications
- Toolkits for data preparation and rule analysis

### Requirements
- Java >= 1.8
- Gradle >= 5.6.4

### Getting Started
We start by learning rules for all relationship types (predicates) in the UWCSE dataset, a small knowledge graph in the academia domain. Please download the dataset from [here](https://www.dropbox.com/s/fscgtbioqa0s06s/UWCSE.zip?dl=1), and unzip into a folder named `data` situated in the GPFL home folder. As GPFL is deeply coupled with the Neo4j core API, it requires data to be presented as a Neo4j database (in this example, the UWCSE database is at folder `data/UWCSE/databases`). Now, run following command to learn rules for all relationship types in UWCSE:
```
gradle run --args="-c data/UWCSE/config.json -r"
```
where option `-c` specifies the location of a GPFL configuration file, and `-r` exectues the chain of rule learning, rule application and evaluation for link prediction.

Once the program finishes, the results will be saved at folder `data/UWCSE/ins3-car3`. The name of the result folder, e.g., `ins3-car3`, can be changed by setting the option `out` in the GPFL configuration file `data/UWCSE/config.json`. Now in the result folder `data/UWCSE/ins3-car3`, file `rules.txt` stores all of the learned rules. To have a look at the top rules, you can execute following command to sort the rules by quality:
```
gradle run --args="-or data/UWCSE/ins3-car3"
```
In the sorted "rule.txt" file, rules are recorded in format:
```
Type  Rule                                                 Conf     HC       VP       Supp  BG
CAR   ADVISED_BY(X,Y) <- PUBLISHES(X,V1), PUBLISHES(Y,V1)  0.09333  0.31343  0.03015  21    220
```
where `conf` is confidence, `HC` head coverage, `VP` validation precision, `supp` support (correct predictions), and `BG` body grounding (total predictions).

To check the quality/type/length distribution of the learned rule, run:
```
gradle run --args="-c data/UWCSE/config.json -ra"
```

To find explanations about predicted and existing facts (test triples) in terms of rules, please check the `verifications.txt` file in result folder. For instance:
```
Head Query: person211	PUBLISHES	title88
Top Answer: 1	person415	PUBLISHES	title88
BAR	PUBLISHES(person415,Y) <- PUBLISHES(V1,Y), PUBLISHES(V1,title12)	0.40426
BAR	PUBLISHES(person415,Y) <- PUBLISHES(V1,Y), PUBLISHES(V1,V2), PUBLISHES(person211,V2)	0.40299
BAR	PUBLISHES(person415,Y) <- PUBLISHES(V1,Y), PUBLISHES(V1,title182)	0.39583

Top Answer: 2	person211	PUBLISHES	title88
BAR	PUBLISHES(person211,Y) <- PUBLISHES(V1,Y), PUBLISHES(V1,V2), PUBLISHES(person284,V2)	0.35294
BAR	PUBLISHES(person211,Y) <- PUBLISHES(V1,Y), PUBLISHES(V1,title259)	0.325
BAR	PUBLISHES(person211,Y) <- PUBLISHES(V1,Y), PUBLISHES(V1,title241)	0.325

Top Answer: 3	person240	PUBLISHES	title88
BAR	PUBLISHES(person240,Y) <- PUBLISHES(V1,Y), PUBLISHES(V1,V2), PUBLISHES(person161,V2)	0.2459
BAR	PUBLISHES(person240,Y) <- PUBLISHES(V1,Y), PUBLISHES(V1,title268)	0.2381
BAR	PUBLISHES(person240,Y) <- PUBLISHES(V1,Y), PUBLISHES(V1,V2), PUBLISHES(person415,V2)	0.17647

Correct Answer: 2	person211	PUBLISHES	title88
BAR	PUBLISHES(person211,Y) <- PUBLISHES(V1,Y), PUBLISHES(V1,V2), PUBLISHES(person284,V2)	0.35294
BAR	PUBLISHES(person211,Y) <- PUBLISHES(V1,Y), PUBLISHES(V1,title259)	0.325
BAR	PUBLISHES(person211,Y) <- PUBLISHES(V1,Y), PUBLISHES(V1,title241)	0.325
```
The `Head Query: person211	PUBLISHES	title88` means the system corrupts known fact `person211	PUBLISHES	title88` into link prediction query `?	PUBLISHES	title88`, and asks the learned rules to suggest candidates to replace `?`. If `person211` is proposed, it is considered as a correct answer, and the rank of the correct answer is used to evaluate the system performance. In this case, the correct answer is ranked top 2 as in:
```
Top Answer: 2	person211	PUBLISHES	title88
BAR	PUBLISHES(person211,Y) <- PUBLISHES(V1,Y), PUBLISHES(V1,V2), PUBLISHES(person284,V2)	0.35294
BAR	PUBLISHES(person211,Y) <- PUBLISHES(V1,Y), PUBLISHES(V1,title259)	0.325
BAR	PUBLISHES(person211,Y) <- PUBLISHES(V1,Y), PUBLISHES(V1,title241)	0.325
```
and the following rules are rules suggesting candidate `person211`, thus can be treated as explanantions on why `person211` publishes paper `title88`.

The `eval_log.txt` in result folder reports the performance of relationships individually.

### GPFL Recipes
In this section, we provide recipes for different scenarios. You can find help information about GPFL by running:
```
gradle run --args="-h"
```

#### Build Fat Jar
```
gradle shadowjar
```
The fat jar file is located at folder `build/libs`

#### Build Neo4j database from a single triple file
Consider the folder you want to save the database is `Foo`, place your triple file in `Foo/data/` and rename it as `train.txt`, then run:
```
gradle run --args="-sbg Foo"
```
The database will be saved at `Foo/databases`

*More to come...*

### Citation
If you are using our code, please cite our paper:
```
@article{gu2020efficient,
  title={Efficient Rule Learning with Template Saturation for Knowledge Graph Completion},
  author={Gu, Yulong and Guan, Yu and Missier, Paolo},
  journal={arXiv preprint arXiv:2003.06071},
  year={2020}
}
```

### License
GPFL is available free of charge for academic research and teaching only. If you intend to use GPFL for commercial purposes then you MUST contact the authors via email y.gu11@newcastle.ac.uk

