#pip install transformers datasets evaluate rouge_score

from transformers import pipeline

from transformers import pipeline, AutoTokenizer, AutoModelForSeq2SeqLM
from transformers import pipeline
import torch
from keybert import KeyBERT

# Check device compatibility and set to CPU if necessary
device = "mps" if torch.backends.mps.is_available() else "cpu"
# Load the model and tokenizer
model_name = "/home/ds/crawl_journal/incremental_crawl/crawl_article/my_awesome_billsum_model"  # Replace with your actual model name/path

tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForSeq2SeqLM.from_pretrained(model_name)

def summarize_text(text, max_length=100, min_length=30):
    """
    Generate a summary for the input text using the fine-tuned model.
    Args:
        text (str): The input text to summarize.
        model: Loaded summarization model.
        tokenizer: Loaded tokenizer.
        max_length (int): Maximum length of the summary.
        min_length (int): Minimum length of the summary.
    Returns:
        str: The generated summary.
    """
    #print('hi')
    inputs = tokenizer.encode("summarize: " + text, return_tensors="pt", max_length=1024, truncation=True)
    #print(inputs)
    outputs = model.generate(inputs, max_length=max_length, min_length=min_length, length_penalty=2.0, num_beams=4, early_stopping=True)
    #print(outputs)
    summary = tokenizer.decode(outputs[0], skip_special_tokens=True)
    #print(summary)
    return summary






# Initialize the pipeline globally
keyword_extractor = pipeline("summarization", model="my_awesome_billsum_model", device=torch.device(device))


def extract_keywords(text, num_keywords=5):
    """
    Extract a specified number of keywords from the text.
    Args:
        text (str): Input text to extract keywords from.
        num_keywords (int): Number of keywords to extract.
    Returns:
        list: A list of extracted keywords.
    """
    global keyword_extractor  # Declare as global at the start of the function

    try:
        # Generate a summary as keywords
        result = keyword_extractor(
            "Extract keywords: " + text,
            max_length=1024,  # Adjust max_length based on expected keyword length
            min_length=num_keywords,  # Ensure at least num_keywords
            do_sample=False
        )[0]['summary_text']

        # Split the result into individual keywords
        keywords = result.split(", ")  # Assuming the model generates comma-separated keywords
        return keywords[:num_keywords]  # Return only the specified number of keywords

    except RuntimeError as e:
        if "isin_Tensor_Tensor_out only works on floating types" in str(e):
            print("Falling back to CPU due to tensor dtype limitation.")
            keyword_extractor = pipeline("summarization", model="my_awesome_billsum_model", device="cpu")
            return extract_keywords(text, num_keywords)  # Retry with CPU
        else:
            raise e


kw_model = KeyBERT()
def extract_keywords_keybert(text):
    keywords = kw_model.extract_keywords(text, keyphrase_ngram_range=(1, 2), stop_words='english', use_mmr=True,
                                         diversity=0.7)
    keyword_str = None
    for items in keywords:
        if float(items[1]) > 0.4:
            # print(str(items[1]) + "::" + items[0])
            if keyword_str == None:
                keyword_str = items[0]
            else:
                keyword_str = keyword_str + "," + items[0]
    print(keyword_str)
    return keyword_str

def main():
    # Example usage
    text = "The quick brown fox jumps over the lazy dog. The dog is sleeping peacefully in the sun. The quick brown fox jumps over the lazy dog. The dog is sleeping peacefully in the sun."
    print(summarize_text(text))
    print(extract_keywords(text))
    print(extract_keywords_keybert(text))
if __name__ == "__main__":
   main()



