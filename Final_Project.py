#!/usr/bin/env python
# coding: utf-8

# # Amazon Customer Review Analysis
# ## By. Andrew Simmons & Jingnan Jin

# ## Schema Reference
# 0. marketplace
# 1. customer_id
# 2. review_id
# 3. product_id
# 4. product_parent
# 5. product_title
# 6. product_category
# 7. star_rating - [1-5]
# 8. helpful_votes
# 9. total_votes
# 10. vine
# 11. verified_purchase
# 12. review_headline
# 13. review_body
# 14. review_date

# In[ ]:


# Valid Modes: TEST or PROD

MODE = "TEST"


# In[ ]:


from collections import defaultdict
import html
import math
from operator import itemgetter
from pathlib import Path
import re
import os


import matplotlib.pyplot as plt
import numpy as np

if MODE == "TEST":
    AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
    AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
    
    import findspark
    findspark.init()

    # create entry points to spark
    try:
        sc.stop()
    except:
        pass
    
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
    
if MODE == "TEST":
    conf = SparkConf().setAppName("AmazonCustomerReviewAnalysis").setMaster("local[*]")

    sc = SparkContext(conf=conf)
    sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", AWS_ACCESS_KEY_ID)
    sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", AWS_SECRET_ACCESS_KEY)

    spark = SparkSession(sparkContext=sc)
elif MODE == "PROD":
    conf = SparkConf().setAppName("AmazonCustomerReviewAnalysis")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sparkContext=sc)


# In[ ]:


"""Constants"""

PLOT_DIMENSIONS = (20, 10)

USELESS_WORDS = set(["a", "about", "above", "across", "after", "afterwards", "again", "against", "all", "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "amoungst", "amount", "an", "and", "another", "any", "anyhow", "anyone", "anything", "anyway", "anywhere", "are", "around", "as", "at", "back", "be", "became", "because", "become", "becomes", "becoming", "been", "before", "beforehand", "behind", "being", "below", "beside", "besides", "between", "beyond", "bill", "both", "bottom", "but", "by", "call", "can", "cannot", "cant", "co", "con", "could", "couldnt", "cry", "de", "describe", "detail", "do", "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven", "else", "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything", "everywhere", "except", "few", "fifteen", "fifty", "fill", "find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", "from", "front", "full", "further", "get", "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how", "however", "hundred", "i", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its", "itself", "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me", "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must", "my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own", "part", "per", "perhaps", "please", "put", "rather", "re", "same", "see", "seem", "seemed", "seeming", "seems", "serious", "several", "she", "should", "show", "side", "since", "sincere", "six", "sixty", "so", "some", "somehow", "someone", "something", "sometime", "sometimes", "somewhere", "still", "such", "system", "take", "ten", "than", "that", "the", "their", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "therefore", "therein", "thereupon", "these", "they", "thick", "thin", "third", "this", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us", "very", "via", "was", "we", "well", "were", "what", "whatever", "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "with", "within", "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves", "..."])


# In[ ]:


if MODE == "PROD":
    dataset = spark.read.load("s3a://amazon-reviews-pds/parquet/")
    dataset = dataset.filter(dataset["marketplace"] == "US")

    categories = [row['product_category'] for row in dataset.select('product_category').distinct().collect()]

    data_categories = {}
    for category in categories:
        data_categories[category] = dataset.filter(dataset['product_category'] == category).rdd.map(tuple).persist()
elif MODE == "TEST":
    #Process list of files to separate by category
    data_dir = Path("sample_data")
    data_files = [
        file
        for file in data_dir.glob("*.tsv.gz")
        if file.name.startswith("amazon_reviews_us_")
    ]

    data_categories = defaultdict(list)
    for file in data_files:
        category_name = file.name[18:-13].replace("_", " ")
        data_categories[category_name].append(file)
    
    # Create mapping of category names to unioned RDD
    for key, value in data_categories.items():
        data_categories[key] = sc.union([sc.textFile(str(file)) for file in value])
    
    # Remove headers from data
    for key, value in data_categories.items():
        data_categories[key] = value.filter(lambda x: not x.startswith("marketplace"))
    
    # Split TSV
    for key, value in data_categories.items():
        data_categories[key] = value.map(lambda x: x.split("\t"))


# In[ ]:


"""Preprocess certain values"""

for key, value in data_categories.items():
    data_categories[key] = value.map(
        lambda x: (
            x[0],
            x[1],
            x[2],
            x[3],
            x[4],
            x[5],
            x[6],
            int(x[7]),
            int(x[8]),
            int(x[9]),
            x[10],
            x[11],
            x[12],  # TODO: Determine if headlines are HTML escaped like the bodies
            html.unescape(x[13]),
            x[14],
        )
    )


# In[ ]:


"""Generate useful RDDs for future use"""

# All categories unioned together
def get_unioned_data(category_data):
    rdds = []
    for value in data_categories.values():
        rdds.append(value)

    return sc.union(rdds)


unioned_data = get_unioned_data(data_categories)


# ## How many records exist in each category?

# In[ ]:


record_counts = []
for key, value in data_categories.items():
    record_counts.append((key, value.count()))

categories, counts = zip(*sorted(record_counts, key=itemgetter(1)))

fig, ax = plt.subplots()
fig.set_size_inches(PLOT_DIMENSIONS)

rects = ax.bar(categories, counts)

ax.set_title("Amazon Review Counts by Category")
ax.set_xlabel("Product Category")
ax.set_ylabel("Review Counts")

for tick in ax.get_xticklabels():
    tick.set_rotation(90)
    
filename = 'records_in_each_category'
fig.savefig(filename + '.pdf')
fig.savefig(filename + '.png')

if MODE == "PROD":
    filename = 'records_in_each_category'
    fig.savefig(filename + '.pdf')
    fig.savefig(filename + '.png')

print(f"Total number of reviews in dataset: {sum(counts)}")


# ## Overall, how satisfied are customers of each product category?

# In[ ]:


average_review_by_category = []
for key, value in data_categories.items():
    star_ratings = value.map(lambda x: int(x[7]))
    average_review_by_category.append((key, star_ratings.mean()))

categories, ratings = zip(*sorted(average_review_by_category, key=itemgetter(1)))

fig, ax = plt.subplots()
fig.set_size_inches(PLOT_DIMENSIONS)

rects = ax.bar(categories, ratings)

ax.set_title("Average Product Category Rating")
ax.set_xlabel("Product Category")
ax.set_ylabel("Average Rating")

for tick in ax.get_xticklabels():
    tick.set_rotation(90)
    
if MODE == "PROD":
    filename = 'satisfaction_in_each_category'
    fig.savefig(filename + '.pdf')
    fig.savefig(filename + '.png')


# ## How does the distribution of review scores change between product category?

# In[ ]:


review_distributions = {}
for key, value in data_categories.items():
    review_distributions[key] = sorted(value.map(lambda x: (x[7], 1)).reduceByKey(lambda x, y: x + y).collect(), key=itemgetter(0))

num_columns = 3

fig, ax = plt.subplots(nrows=math.ceil(len(review_distributions.keys()) / num_columns),
                       ncols=num_columns)
fig.set_size_inches((20, 100))
ax = ax.flatten()

for i, (key, value) in enumerate(review_distributions.items()):
    stars, counts = zip(*value)
    
    rects = ax[i].bar(stars, counts)
    
    # TODO: Calculate mean more efficiantly
    total = 0
    for j in range(len(stars)):
        total += stars[j] * counts[j]
    mean = total / sum(counts)
    
    # TODO: Include a median line
    mean_line = ax[i].axvline(mean, color='red', linestyle='--')
    ax[i].legend([mean_line], ['Mean Category Rating'])
    
    ax[i].set_title(f'Ratings Frequency Distribution for {key} category')
    ax[i].set_xlabel('Star Rating')
    ax[i].set_ylabel('Rating Count')

if MODE == "PROD":
    filename = 'review_scores_distribution_by_category'
    fig.savefig(filename + '.pdf')
    fig.savefig(filename + '.png')


# ## What words are most used in each category at each rating for review headlines?

# In[ ]:


"""
category: {
    1: [n most common words],
    2: ...
},
...
"""

DEFAULT_REVIEWS = {"One Star", "Two Stars", "Three Stars", "Four Stars", "Five Stars"}


def remove_default_headlines(headline):
    """Remove default headlines"""
    if headline in DEFAULT_REVIEWS:
        return False
    return True


def preprocess(headline):

    # Remove HTML breaks
    headline = re.sub(r"<br \/>", " ", headline)

    # Convert multiple spaces to a single space
    headline = re.sub(r"\s+", " ", headline, flags=re.I)

    # Remove punctuation that is found at the end of words
    headline = re.sub(r"[,.!?]", "", headline)

    # Remove apostrophies as to group together both spellings of a word
    headline = re.sub(r"'", "", headline)

    return headline.lower()


def remove_useless_words(headline_word):
    """Remove stop words and elipses"""

    if headline_word in USELESS_WORDS:
        return False
    return True


def remove_empty_words(headline_word):
    if len(headline_word) == 0:
        return False
    return True


def remove_censored_swear_words(headline_word):
    """Amazon appears to have censored swear words by keeping the first character
    and replacing the other characters with astrisks. These are not useful to us.
    """

    if re.match("^[a-z]\*+$", headline_word) is not None:
        return False
    return True


def remove_words_without_alphanumeric(headline_word):
    if re.match("[a-z0-9]", headline_word) is None:
        return False
    return True


# TODO: This doesn't seem to work 100% of the time
def remove_numbers(headline_word):
    """Remove words that consist of nothing but numbers"""
    if re.match("^[0-9]+$", headline_word) is not None:
        return False
    return True


def remove_single_letter_words(headline_word):
    if len(headline_word) == 1:
        return False
    return True


POSSIBLE_RATINGS = [1, 2, 3, 4, 5]
resulting_word_frequencies = defaultdict(dict)

for key, value in data_categories.items():

    rating_words = {}

    for i in POSSIBLE_RATINGS:

        reviews = value.filter(lambda x: x[7] == i)

        headlines = (
            reviews.map(lambda x: x[12])
            .filter(remove_default_headlines)
            .map(preprocess)
        )

        headline_words = (
            headlines.flatMap(lambda x: x.split())
            .filter(remove_useless_words)
            .filter(remove_empty_words)
            .filter(remove_censored_swear_words)
            .filter(remove_words_without_alphanumeric)
            .filter(remove_single_letter_words)
        )

        word_frequencies = (
            headline_words.map(lambda x: (x, 1))
            .reduceByKey(lambda x, y: x + y)
            .sortBy(lambda x: x[1], ascending=False)
            .map(lambda x: x[0])
        )

        resulting_word_frequencies[key][i] = word_frequencies.take(3)

# Output results

for category in resulting_word_frequencies.keys():

    print(f"{category}:")

    for rating in POSSIBLE_RATINGS:

        words = resulting_word_frequencies[category][rating]

        print(f"  {rating}: {list(words)}")


# ## What words are most used in each category at each rating for review bodies?

# In[ ]:


resulting_word_frequencies = defaultdict(dict)
for key, value in data_categories.items():
    rating_words = {}
    for i in POSSIBLE_RATINGS:
        reviews = value.filter(lambda x: x[7] == i)
        headlines = reviews.map(lambda x: x[13]).map(preprocess)

        headline_words = (
            headlines.flatMap(lambda x: x.split())
            .filter(remove_useless_words)
            .filter(remove_empty_words)
            .filter(remove_censored_swear_words)
            .filter(remove_words_without_alphanumeric)
            .filter(remove_single_letter_words)
        )
        word_frequencies = (
            headline_words.map(lambda x: (x, 1))
            .reduceByKey(lambda x, y: x + y)
            .sortBy(lambda x: x[1], ascending=False)
            .map(lambda x: x[0])
        )

        resulting_word_frequencies[key][i] = word_frequencies.take(3)

# Output results
for category in resulting_word_frequencies.keys():
    print(f"{category}:")
    for rating in POSSIBLE_RATINGS:
        words = resulting_word_frequencies[category][rating]
        print(f"  {rating}: {list(words)}")


# ## How does the average rating in a category change between verified and unverified purchasers?

# In[ ]:


mean_ratings = []
for key, value in data_categories.items():
    all_ratings_mean = value.map(lambda x: x[7]).mean()
    verified_ratings_mean = (
        value.filter(lambda x: x[11] == "Y").map(lambda x: x[7]).mean()
    )
    unverified_ratings_mean = (
        value.filter(lambda x: x[11] == "N").map(lambda x: x[7]).mean()
    )

    mean_ratings.append(
        (key, all_ratings_mean, verified_ratings_mean, unverified_ratings_mean)
    )


num_columns = 3

fig, ax = plt.subplots(
    nrows=math.ceil(len(mean_ratings) / num_columns), ncols=num_columns
)

fig.set_size_inches((20, 100))
ax = ax.flatten()

labels = ["All", "Verified", "Unverified"]

for i, category in enumerate(mean_ratings):
    rects = ax[i].bar(labels, category[1:], color=["orange", "blue", "green"])

    ax[i].set_title(f'Average Rating in "{category[0]}" Category')
    ax[i].set_xlabel("Review Type")
    ax[i].set_ylabel("Average Rating")

if MODE == "PROD":
    filename = 'verified_vs_unverified_ratings'
    fig.savefig(filename + '.pdf')
    fig.savefig(filename + '.png')


# In[ ]:


print(
    "=== Differences between verified review average and unverified review average by stars ===\n"
)

# (category_name, average_difference)
average_difference = []
for category in mean_ratings:
    category_name = category[0]
    verified_review_average = category[2]
    unverified_review_average = category[3]

    average_difference.append(
        (
            category_name,
            abs(abs(verified_review_average) - abs(unverified_ratings_mean)),
        )
    )

# Print differences by category
for category in average_difference:
    print(f"{category[0]}: {category[1]}")


# Print total average differences
_, difference = zip(*average_difference)
print(f"Overall: {sum(difference) / len(difference)}")


# ## What review ratings to customers find the most helpful?

# In[ ]:


# (rating, (helpful_vote_percentage, 1))
helpful_vote_percentage = unioned_data.filter(lambda x: x[9] != 0).map(
    lambda x: (x[7], (x[8] / x[9], 1))
)

# (rating, average_helpful_vote_percentage)
average_helpful_vote_percentage = helpful_vote_percentage.reduceByKey(
    lambda x, y: (x[0] + y[0], x[1] + y[1])
).map(lambda x: (x[0], x[1][0] / x[1][1]))

helpful_averages = sorted(average_helpful_vote_percentage.collect())
rating, helpful_average = zip(*helpful_averages)

fig, ax = plt.subplots()
fig.set_size_inches(PLOT_DIMENSIONS)

rects = ax.bar(rating, helpful_average)

ax.set_title("Average Helpfulness by Review Rating")
ax.set_xlabel("Rating")
ax.set_ylabel("Average Helpfulness")

if MODE == "PROD":
    filename = 'rating_helpfulness'
    fig.savefig(filename + '.pdf')
    fig.savefig(filename + '.png')


# ## Does the Amazon Vine program influence customer reviews?

# In[ ]:


average_vine_rating = (
    unioned_data.filter(lambda x: x[10] == "Y")
    .map(lambda x: (x[7], 1))
    .reduce(lambda x, y: (x[0] + y[0], x[1] + y[1]))
)
average_vine_rating = average_vine_rating[0] / average_vine_rating[1]

average_nonvine_rating = (
    unioned_data.filter(lambda x: x[10] == "N")
    .map(lambda x: (x[7], 1))
    .reduce(lambda x, y: (x[0] + y[0], x[1] + y[1]))
)
average_nonvine_rating = average_nonvine_rating[0] / average_nonvine_rating[1]

difference = abs(abs(average_vine_rating) - abs(average_nonvine_rating))

print(f"Difference between vine and non-vine average: {difference}")

fig, ax = plt.subplots()
fig.set_size_inches(PLOT_DIMENSIONS)

rects = ax.bar(["Vine", "Non-Vine"], [average_vine_rating, average_nonvine_rating])

ax.set_title("Vine vs Non-Vine Customer Review Average")
ax.set_xlabel("Review Type")
ax.set_ylabel("Average Rating")

for tick in ax.get_xticklabels():
    tick.set_rotation(90)
    
if MODE == "PROD":
    filename = 'vine_rating_influence'
    fig.savefig(filename + '.pdf')
    fig.savefig(filename + '.png')


# ## Is it possible to predict the rating associated with a review by the sentiment of it’s words?

# In[ ]:


"""We predict that the sentiment of a word can be derived by an average of the ratings of the reviews that it occured in.
this will act as a weight which can be used to preduct the rating of a given review
"""


def word_to_rating(x):
    return_list = []

    rating = x[0]
    words = x[1]

    for word in words:
        return_list.append((word, rating))

    return return_list


def clean_words(pair_rdd):
    words = pair_rdd[1]

    words = filter(remove_useless_words, words)
    words = filter(remove_empty_words, words)
    words = filter(remove_censored_swear_words, words)
    words = filter(remove_words_without_alphanumeric, words)
    words = filter(remove_single_letter_words, words)

    return words


# (rating, [word1, word2, ...])
rating_to_review_words = (
    unioned_data.map(lambda x: (x[7], x[13]))
    .mapValues(preprocess)
    .map(lambda x: (x[0], x[1].split()))
    .filter(clean_words)
)

# [(word1, rating), (word2, rating), ...]
review_words_to_ratings = rating_to_review_words.flatMap(word_to_rating)

# [(word1, average_rating), (word2, average_rating), ...]
average_word_rating = (
    review_words_to_ratings.map(lambda x: (x[0], (x[1], 1)))
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    .map(lambda x: (x[0], x[1][0] / x[1][1]))
)


word_frequency = (
    review_words_to_ratings.map(lambda x: (x[0], 1))
    .reduceByKey(lambda x, y: x + y)
    .collectAsMap()
)
total_num_words = review_words_to_ratings.map(lambda x: x[0]).count()


word_weights = average_word_rating.collectAsMap()


def predict_rating_from_review(review):
    words = review.split()

    weights = []
    for word in words:
        if word in word_weights:
            weights.append(word_weights[word])

    # If none of the words in the input are in the weight set, return None instead of a prediction
    if len(weights) == 0:
        return None

    return sum(weights) / len(weights)


examples = [
    "really amazing product i love it so much absolutely incredible",
    "it was alright not the best but not the worst",
    "terrible i feel scammed i will return this this should be illegal",
    "worst terrible refund",
    "terrible",
    "bad",
    "good",
    "perfect",
    "thiswordisnotinthedataset",
]

print("=== Rating predictions ===")
for example in examples:
    print(f'Review text: "{example}"')
    print(f"Predicted Rating: {predict_rating_from_review(example)}")
    print()


# In[ ]:




