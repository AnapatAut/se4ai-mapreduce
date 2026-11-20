# Reflection

Name: Anapat

Answer each question in 2–3 sentences.

**1. In Task 1, what did the shuffle do with the pairs your mapper produced? Why can the reducer only run once all mappers have finished?**

The shuffle group all pairs with the same image path together so each of the 719 paths got one list with all its 1s. The reducer need all the values for a path to get the right total and any mapper could still send one more pair until every mapper is done.

**2. In Task 3, why did the original job give different answers with 1 and 4 workers? Why does moving the de-duplication into the reducer fix it, whatever the number of workers or partitions?**

The already_sent set lived inside each process so with 1 worker one set was shared by all partitions but with 4 workers each process had its own set and which pages got skipped depended on how the work was split. When the mapper only look at one line it sends the same pairs no matter how the work is split and the reducer gets every page for the hour and counts each one only once.

**3. The photo service from the lecture releases a new object-detection model and must re-tag 4 trillion stored photos. Which part of that job is the map step, is there a reduce step, and why would you run the computation on the machines that store the photos?**

The map step runs the new model on each photo and sends out the photo ID with its tags. There is no reduce step unless we also want to count how many photos have each tag. We run it on the machines that store the photos because moving petabytes of photos over the network cost much more than sending the model to them.
