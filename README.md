# flopo-data-pipeline

Scripts to convert the main article datasets used in the FLOPO project to CONLL-CSV and index them in the Octavo index service. 

One of the four source datasets is not openly available, while the others have license restrictions prohibiting distributing them 
directly as part of this release.

The status per dataset is as follows:
 * Helsingin Sanomat: not open
 * Iltalehti: https://tekniikan-misc.s3-eu-west-1.amazonaws.com/il_articles/iltalehti_articles_2006_2019.tar.gz . No license specified, thus status unclear.
 * STT: https://www.kielipankki.fi/aineistot/stt-fi/. Available for research use.
 * Yle: https://www.kielipankki.fi/aineistot/ylenews/. Available for academic use.

For understanding the workflow, consult the Luigi [workflow.py](code/workflow.py).
