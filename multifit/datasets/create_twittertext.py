"""
Script to create small and large WikiText datasets from Wikipedia articles in
any language that were downloaded with `prepare_wiki.sh`.
Articles are tokenized using the Moses tokenizer. Articles with least than
100 tokens are removed.
"""
import argparse
from pathlib import Path
import json
import re

from shutil import copyfile

from sacremoses import MosesTokenizer

import emoji
emojis = list(emoji.UNICODE_EMOJI.keys())

emoji_pattern = re.compile(
    "["
    u"\U0001F600-\U0001F64F"  # emoticons
    u"\U0001F300-\U0001F5FF"  # symbols & pictographs
    u"\U0001F680-\U0001F6FF"  # transport & map symbols
    u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
    u"\U00002760-\U0000276F"  # emoticons
    "]+", flags=re.UNICODE
)

emoji_regex = re.compile("[" + u"".join(emojis) + "]", flags=re.UNICODE)

url_regex = re.compile('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\), ]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
pic_url_regex = re.compile('pic\.twitter\.com\/\w+')
multi_dot_regex = re.compile('( *(\.( *))+)')
mention_regex = re.compile('@(\w{1,15})( )*(:)*')
hashtag_regex = re.compile('#\w*[a-zA-Z]+\w*')
multi_space_regex = re.compile('(\s+|\n+)')

def clean_mention(match):
    clean_str = [s if s.isalpha() else ' ' for s in match.group(1)]
    return ''.join(clean_str)

def clean_line(line):
    if line[:3] == 'RT ':
        line = line[3:]
    cleaned_line = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\), ]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', line)
    cleaned_line = re.sub(r'pic\.twitter\.com\/\w+','',cleaned_line)
    cleaned_line = re.sub(r'( *(\.( *))+)',' . ', cleaned_line)
    #cleaned_line = re.sub(r'@(\w{1,15})( )*(:)*',' TWITTERMENTION ',cleaned_line)
    #cleaned_line = re.sub(r'#\w*[a-zA-Z]+\w*',' TWITTERHASHTAG ', cleaned_line)
    cleaned_line = re.sub(r'@(\w{1,15})( )*(:)*',r' \1 ',cleaned_line)
    cleaned_line = re.sub(r'#\w*[a-zA-Z]+\w*',' ', cleaned_line)
    #cleaned_line = emoji_regex.sub(' TWITTEREMOJI ', cleaned_line)
    cleaned_line = re.sub(r'(\s+|\n+)',' ',cleaned_line)
    return cleaned_line

def clean_line_mod(line):
    if line[:3] == 'RT ':
        line = line[3:]
    cleaned_line = url_regex.sub('', line)
    cleaned_line = pic_url_regex.sub('', cleaned_line)
    cleaned_line = multi_dot_regex.sub(' . ', cleaned_line)
    cleaned_line = mention_regex.sub(clean_mention, cleaned_line)
    cleaned_line = hashtag_regex.sub(' ', cleaned_line)
    cleaned_line = multi_space_regex.sub(' ', cleaned_line)
    return cleaned_line

def get_text_from_tweet(tweet):
    if 'retweeted_status' in tweet.keys():
        if 'extended_tweet' in tweet['retweeted_status']:
            text = tweet['retweeted_status']['extended_tweet']['full_text']
        else:
            text = tweet['retweeted_status']['text']
    else:
        if 'extended_tweet' in tweet:
            text = tweet['extended_tweet']['full_text']
        else:
            text = tweet['text']
    return text

def get_texts(root):
    for tweets_file in root.iterdir():
        with open(tweets_file, encoding='utf-8') as f_in:
            for line in f_in:
                tweet = json.loads(line)
                text = get_text_from_tweet(tweet)
                text = clean_line_mod(text)
                yield text

def write_tweettext(file_path, text_iter, mt, num_tokens, mode='w', num_tokens_tweet_min=10):
    total_num_tokens = 0
    print(f'Writing to {file_path}...')
    i = 0
    with open(file_path, mode, encoding='utf-8') as f_out:
        for i, text in enumerate(text_iter):
            num_tokens_tweet = 0  # count the number of tokens in an article
            #protected_patterns=['twitter_mention', 'twitter_hashtag']
            protected_patterns=[]
            tokenized = mt.tokenize(text.strip(), return_str=True, protected_patterns=protected_patterns)
            tokens = tokenized.split(' ')
            tokens = [token for token in tokens if token]
            num_tokens_tweet += len(tokens) + 1

            if num_tokens_tweet < num_tokens_tweet_min:
                # only use tweets that have at least num_tokens_tweet_min tokens
                continue

            f_out.write(tokenized + '\n')

            total_num_tokens += num_tokens_tweet + 1
            if num_tokens is not None and total_num_tokens > num_tokens:
                break
            if i % 10000 == 0 and i > 0:
                print('Processed {:,} documents. Total # tokens: {:,}.'.format(i, total_num_tokens))
    print('{}. # documents: {:,}. # tokens: {:,}.'.format(
        file_path, i, total_num_tokens))


def main(args):
    input_path = Path(args.input)
    output = Path(args.output)
    assert input_path.exists(), f'Error: {input_path} does not exist.'
    output.mkdir(exist_ok=True)

    mt = MosesTokenizer(args.lang)

    sml_tweets = output / f'{args.lang}-2'
    lrg_tweets = output / f'{args.lang}-100'
    all_tweets = output / f'{args.lang}-all'
    sml_tweets.mkdir(exist_ok=True)
    lrg_tweets.mkdir(exist_ok=True)
    all_tweets.mkdir(exist_ok=True)

    text_iter = get_texts(input_path)

    splits = ['train', 'valid', 'test']
    token_nums = [2000000, 200000, 200000]
    for split, token_num in zip(splits, token_nums):
        sml_file_path = sml_tweets / f'{args.lang}.tweets.{split}.tokens'
        write_tweettext(sml_file_path, text_iter, mt, token_num, num_tokens_tweet_min=args.tokens_min)
        lrg_file_path = lrg_tweets / f'{args.lang}.tweets.{split}.tokens'
        all_file_path = all_tweets / f'{args.lang}.tweets.{split}.tokens'
        # copy the content of the small file to the large file
        print(f'Copying {sml_file_path} to {lrg_file_path} & {all_file_path}.')
        copyfile(sml_file_path, lrg_file_path)
        copyfile(sml_file_path, all_file_path)

    # add the new articles to the existing ones
    lrg_tweets_train = lrg_tweets / f'{args.lang}.tweets.train.tokens'
    write_tweettext(lrg_tweets_train, text_iter, mt, 98000000, mode='a', num_tokens_tweet_min=args.tokens_min)
    all_tweets_train = all_tweets / f'{args.lang}.tweets.train.tokens'
    copyfile(lrg_tweets_train, all_tweets_train)
    write_tweettext(all_tweets_train, text_iter, mt,  None, mode='a', num_tokens_tweet_min=args.tokens_min)

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', required=True,
                        help='the directory where the Twitter jsonlines files '
                             'exist')
    parser.add_argument('-o', '--output', required=True,
                        help='the output directory where the merged data '
                             'should be saved')
    parser.add_argument('-l', '--lang', required=True,
                        help='the iso code of the language of the documents, '
                             'e.g. en, fr, de, etc.')
    parser.add_argument('-t', '--tokens_min', required=False, type=int, default=10,
                        help='the minimal number of tokens in an article')
    args = parser.parse_args()
    main(args)

