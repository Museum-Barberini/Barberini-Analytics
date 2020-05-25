# ABSA

## Technical todos

- Ideas for polarity report:
  * Skip neutral words
- In `CollectPostNgrams`, do not skip too many stopwords
  * If we skip all stopwords, n-grams such as "van Gogh" are skipped, too (stopword: "van")
  * If we skip only stopwords in 1-grams, n-grams such as "the museum" appear, too

## Long-term todos

- `post_words.pbit` is very technical. Restrict it to visualizations that are relevant for the end-user when research is done.
