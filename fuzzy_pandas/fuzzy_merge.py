import csvmatch
import pandas as pd

def fuzzy_merge(df1,
          df2,
          on=None,
          left_on=None,
          right_on=None,
          keep=None,
          keep_left='all',
          keep_right='all',
          method='exact',
          threshold=0.6,
          training_file=None, #TODO: EITHER ADD A POSITIONAL ARGUMENT FOR THE LIST OF LIST OF TRAINING FILES: [trainingmatches1.csv,trainingmatches2.csv,...]
          **kwargs):       #TODO: OR, YOU CAN ADD IT AS A KEYWORD ARGUMENT WITH THE REST OF THE OPTIONAL ARGUMENTS.
    """Fuzzy matching between two dataframes

    Parameters
    ----------
    left : DataFrame
    right : DataFrame
        Object to merge left with
    on : str or list
        Column names to compare. These must be found in both DataFrames.
    left_on : str or list
        Column names to compare in the left DataFrame.
    right_on : str or list
        Column names to compare in the right DataFrame.
    keep : str { 'all', 'match' }
        Overrides keep_left and keep_right
    keep_left : str or list, default 'all'
        List of columns to preserve from the left DataFrame.
        If 'all', preserve all columns.
        If 'match', preserve left_on matching) column.
        If any other string, just keeps that one column.
    keep_right : str or list, default 'all'
        List of columns to preserve from the right DataFrame.
        If 'all', preserve all columns. Defaults to right_on.
        If 'match', preserve right_on (matching) column.
        If any other string, just keeps that one column.
    method : str or list, default 'exact'
        Perform a fuzzy match, and an optional specified algorithm.
        Multiple algorithms can be specified which will apply to each field
        respectively.

        * exact: exact matches
        * levenshtein: string distance metric
        * jaro: string distance metric
        * metaphone: phoenetic matching algorithm
        * bilenko: prompts for matches

    threshold : float or list, default 0.6
        The threshold for a fuzzy match as a number between 0 and 1
        Multiple numbers will be applied to each field respectively
    ignore_case : bool, default False
        Ignore case (default is case-sensitive)
    ignore_nonalpha : bool, default False
        Ignore non-alphanumeric characters
    ignore_nonlatin : bool, default False
        Ignore characters from non-latin alphabets
        Accented characters are compared to their unaccented equivalent
    ignore_order_words : bool, default False
        Ignore the order words are given in
    ignore_order_letters : bool, default False
        Ignore the order the letters are given in, regardless of word order
    ignore_titles : bool, default False
        Ignore a predefined list of name titles (such as Mr, Ms, etc)
    join : { 'inner', 'left-outer', 'right-outer', 'full-outer' }

    Returns
    -------
    pd.DataFrame
        a DataFrame of matchine rows
    """

    #there is a transformation that happens here to the pandas dataframe thats
    #passed in... that means that i will probably have to do some transformation

    data1 = df1.values.tolist()
    headers1 = list(df1.columns)

    data2 = df2.values.tolist()
    headers2 = list(df2.columns)

    #TODO: PREPARE_TRAINING() TAKES IN A FILE NAME AND THEN USES:
    """
    deduper.prepare_training(data_d, 150000, .5)

    with open(training_file, 'rb') as f:
        deduper.prepare_training(data_d, training_file=f)
        uncertainPairs()
    """

    #THAT TO DO THE PREPARATION. SO, IF I PASS IN A LIST OF MATCH FILES,
    #THAT MEANS THAT I NEED TO COLLATE THE MATCHES INTO ONE BIG "MATCH"
    #FILE. I CAN EITHER CHOOSE TO DO THAT IN HERE, OR OUTSIDE OF THIS
    #PACKAGE.
        #TODO: IN THIS COLLATE FUNCTION, I ALSO NEED SOME WAY TO DEAL WITH
        #MULTIPLE "PAIRS" SO THIS MEANS.. WOULD I JUST PUT IN A PERMUTATION
        #OF ALL PAIRS... ? (INTEL/INTC) (INTEL/INTC CORP) (INTC CORP/INTC)?
        #NEED TO LOOK THIS UP. MAYBE ASK DAN THE BEST WAY TO PASS THIS INFO IN

    if not isinstance(threshold, list):
        threshold = [threshold]

    if on:
        left_on = on
        right_on = on

    if not isinstance(left_on, list):
        left_on = [left_on]

    if not isinstance(right_on, list):
        right_on = [right_on]

    if keep:
        keep_left = keep
        keep_right = keep

    if keep_left == 'all':
        keep_left = headers1
    if keep_right == 'all':
        keep_right = headers2

    if keep_left == 'match':
        keep_left = left_on
    if keep_right == 'match':
        keep_right = right_on

    if isinstance(keep_left, str):
        keep_left = [keep_left]

    if isinstance(keep_right, str):
        keep_right = [keep_right]

    output = []
    output.extend(['1.' + col for col in (keep_left or left_on)])
    output.extend(['2.' + col for col in (keep_right or right_on)])

    if not isinstance(method, list):
        method = [method]

    output = kwargs.pop('output', output)
    """
    so this means that if i dont list what the ouput key is in kwargs...
    then it will use the output that was created up there. Basically, the
    idea is that .pop() will look for that key 'output' in kwargs, but if
    its not there... then it will take the output created up there. So, long
    story short I dont actually need to know what the code does up there.

    """


    results, keys = csvmatch.run(  #TODO: PUT IN THE TRAINING MATCHES LIST AS AN ARGUMENT HERE:
        data1,
        headers1,
        data2,
        headers2,
        fields1=left_on,
        fields2=right_on,
        thresholds=threshold,
        output=output,
        methods=method,
        training_file=None, #pass it in as None first
        #training_list = [trainingmatches1.csv,trainingmatches2.csv,...]
        **kwargs)
        #So, the kwargs that is right here.... is directly passed from the beginning of this method. Its not used at all.
        #no the above statement isn't true. It has some output thing that goes on?
        #yes it is true, I could pass something else than output into there

    return pd.DataFrame(results, columns=keys)
