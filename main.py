from datetime import datetime

import requests
import pandas as pd
from os.path import exists

# copied from query at:
# https://www.consilium.europa.eu/en/general-secretariat/corporate-policies/transparency/open-data/voting-results/
SPARQL_QUERY_FOR_ALL_VOTES = "PREFIX acts: <http://data.consilium.europa.eu/id/acts/>  PREFIX tax: <http://data.consilium.europa.eu/id/taxonomy/>  PREFIX codi: <http://data.consilium.europa.eu/def/codi/>  PREFIX skos:<http://www.w3.org/2004/02/skos/core#>  PREFIX dct: <http://purl.org/dc/terms/>  PREFIX foaf: <http://xmlns.com/foaf/0.1/>  PREFIX owl: <http://www.w3.org/2002/07/owl#>  PREFIX votpos: <http://data.consilium.europa.eu/id/taxonomy/votingposition>  SELECT distinct ?voteProc ?votingRuleId ?policyAreaId ?voteOnDocumentNumber ?votingInstCode ?legisProcId ?decisionDate  group_concat(distinct ?councilActionId;separator='|') as ?councilActionIdGrouped ?meetingSessionNumber ?councilConfigurationId  ?docExpression ?docTitle ?registerPage ?actNumber ?actTypeId  FROM <http://data.consilium.europa.eu/id/dataset/VotingResults>  FROM <http://data.consilium.europa.eu/id/dataset/PublicRegister>  WHERE {    ?voteProc a codi:VotingProcedure.      ?voteProc codi:votingRule ?votingRule.      ?votingRule skos:notation ?votingRuleId    optional {      ?voteProc codi:policyArea ?policyArea.      ?policyArea skos:notation ?policyAreaId    }.    optional {       ?voteProc codi:voteOn ?voteOn.      FILTER (CONTAINS(STR(?voteOn), 'INIT')).      ?voteOn codi:document_number ?voteOnDocumentNumber.      optional { ?voteOn codi:act_number ?actNumber. }.      optional {        ?voteOn codi:actType ?actType.        ?actType skos:notation ?actTypeId.      }.      optional { ?voteOn foaf:page ?registerPage. }      ?voteOn codi:expressed ?docExpression.      ?docExpression dct:title ?docTitle.      ?docExpression dct:language ?docLanguage.      FILTER ( lang(?docTitle) = 'en' || lang(?docTitle) = 'en' )    }.    optional { ?voteProc codi:forInterInstitutionalCode ?votingInstCode }.    optional {      ?voteProc codi:legislativeProcedure ?legisProc.      ?legisProc skos:notation ?legisProcId.    }.    ?voteProc codi:hasVoteOutcome ?voteDecision.    ?voteDecision dct:dateAccepted ?decisionDate.    optional {      ?voteDecision codi:councilAction ?councilAction.      ?councilAction skos:notation ?councilActionId.    }.    optional {      ?meeting codi:appliesProcedure ?voteProc.      optional { ?meeting codi:meetingsessionnumber ?meetingSessionNumber. }      optional {        ?meeting codi:configuration ?meetingConfig.        ?meetingConfig a skos:Concept.        ?meetingConfig skos:notation ?councilConfigurationId.      }    }  } ORDER BY DESC(?decisionDate), ?votingInstCode"
SPARQL_QUERY_FOR_ALL_VOTE_RESULTS = "PREFIX acts: <http://data.consilium.europa.eu/id/acts/>  PREFIX tax: <http://data.consilium.europa.eu/id/taxonomy/>  PREFIX codi: <http://data.consilium.europa.eu/def/codi/>  PREFIX skos:<http://www.w3.org/2004/02/skos/core#>  PREFIX dct: <http://purl.org/dc/terms/>  PREFIX foaf: <http://xmlns.com/foaf/0.1/>  PREFIX owl: <http://www.w3.org/2002/07/owl#>  PREFIX votpos: <http://data.consilium.europa.eu/id/taxonomy/votingposition>  SELECT distinct ?voteProc ?voteDecision ?decisionDate  group_concat(distinct ?countryCodeInFavour;separator='|') as ?countryCodeInFavourGrouped  group_concat(distinct ?countryCodeAgainst;separator='|') as ?countryCodeAgainstGrouped  group_concat(distinct ?countryCodeAbstained;separator='|') as ?countryCodeAbstainedGrouped  group_concat(distinct ?countryCodeNotParticipating;separator='|') as ?countryCodeNotParticipatingGrouped  FROM <http://data.consilium.europa.eu/id/dataset/VotingResults>  WHERE {     ?voteProc a codi:VotingProcedure.     ?voteProc codi:hasVoteOutcome ?voteDecision.     ?voteDecision codi:hasVotingPosition ?countryVote_uri .     optional {        ?countryVote_uri codi:votingposition <http://data.consilium.europa.eu/id/taxonomy/votingposition/votedagainst>.        ?countryVote_uri codi:country ?countryVoteAgainst_uri.        ?countryVoteAgainst_uri skos:notation ?countryCodeAgainst.     }     optional {        ?countryVote_uri codi:votingposition <http://data.consilium.europa.eu/id/taxonomy/votingposition/votedinfavour>.        ?countryVote_uri codi:country ?countryVoteInFavour_uri.        ?countryVoteInFavour_uri skos:notation ?countryCodeInFavour.     }     optional {        ?countryVote_uri codi:votingposition <http://data.consilium.europa.eu/id/taxonomy/votingposition/abstained>.        ?countryVote_uri codi:country ?countryAbstained_uri.        ?countryAbstained_uri skos:notation ?countryCodeAbstained.     }     optional {        ?countryVote_uri codi:votingposition <http://data.consilium.europa.eu/id/taxonomy/votingposition/notparticipating>.        ?countryVote_uri codi:country ?countryNotParticipating_uri.        ?countryNotParticipating_uri skos:notation ?countryCodeNotParticipating.     }  } "

VOTE_CODE_YES = "Y"
VOTE_CODE_NO = "N"
VOTE_CODE_ABSTAIN = "A"
VOTE_CODE_NOT_PARTICIPATED = "0"
VOTE_VALUES_BY_ALL_VOTE_RESULTS_KEYS = {
    "countryCodeInFavourGrouped": VOTE_CODE_YES,
    "countryCodeAgainstGrouped": VOTE_CODE_NO,
    "countryCodeAbstainedGrouped": VOTE_CODE_ABSTAIN,
    "countryCodeNotParticipatingGrouped": VOTE_CODE_NOT_PARTICIPATED,
}
VOTE_CAST = [VOTE_CODE_YES, VOTE_CODE_NO, VOTE_CODE_ABSTAIN]

VOTES_BY_MEMBER_STATES_FILENAME = 'votes_by_member_states.csv'
SAME_VOTINGS_PARTICIPATED_FILENAME = 'votings_together.csv'
SAME_VOTES_CAST_FILENAME = 'same_votes.csv'
SAME_VOTE_PERCENTAGES_FILENAME = 'same_vote_percentages.csv'
GROUP_VOTE_DATA_FILENAME = 'group_vote_data.csv'

ALL_MEMBER_STATE_CODES = [
    'AT', 'BE', 'BG', 'CY', 'CZ', 'DE', 'DK', 'EE', 'EL', 'ES', 'FI', 'FR', 'HR', 'HU', 'IE', 'IT', 'LT', 'LU', 'LV',
    'MT', 'NL', 'PL', 'PT', 'RO', 'SE', 'SI', 'SK', 'UK'
]


def fetch_query_results(query):
    response = requests.post("https://data.consilium.europa.eu/sparql", data={
        "query": query,
        "format": "application/sparql-results+json",
        "timeout": "0"

    })
    return response.json()['results']['bindings']


def calculate_votes_by_member_states(votings):
    votes_by_member_states = pd.DataFrame(columns=ALL_MEMBER_STATE_CODES)
    for voting in votings:
        row_data = {'date': voting['decisionDate']}
        for vote_key in VOTE_VALUES_BY_ALL_VOTE_RESULTS_KEYS.keys():
            vote_value = VOTE_VALUES_BY_ALL_VOTE_RESULTS_KEYS[vote_key]
            voting_countries_unparsed = voting[vote_key]["value"]
            voting_member_states = voting_countries_unparsed.split("|")
            for member_state in voting_member_states:
                if member_state != '':
                    row_data[member_state] = vote_value
        votes_by_member_states = votes_by_member_states.concat(row_data, ignore_index=True)
    return votes_by_member_states


def get_cacheable_data(filename, data_extractor_function, *parameters):
    file_path = f'csv/{filename}'
    if exists(file_path):
        return pd.read_csv(file_path)
    else:
        data = data_extractor_function(*parameters)
        data.to_csv(file_path)
        return data


# TODO consider decoupling loop, transform to get_cacheable_data (will hit performance)
def get_processed_vote_data(votes_by_member_states):
    same_votings_participated_path = f'csv/{SAME_VOTINGS_PARTICIPATED_FILENAME}'
    same_votes_cast_path = f'csv/{SAME_VOTES_CAST_FILENAME}'
    if exists(same_votings_participated_path) and exists(same_votes_cast_path):
        return pd.read_csv(same_votings_participated_path), pd.read_csv(same_votes_cast_path),
    else:
        votings_together = pd.DataFrame(columns=ALL_MEMBER_STATE_CODES, index=ALL_MEMBER_STATE_CODES, data=0)
        same_votes = pd.DataFrame(columns=ALL_MEMBER_STATE_CODES, index=ALL_MEMBER_STATE_CODES, data=0)
        for index, row in votes_by_member_states.iterrows():
            for member_state_1 in ALL_MEMBER_STATE_CODES:
                for member_state_2 in ALL_MEMBER_STATE_CODES:
                    if member_state_1 != member_state_2 and row[member_state_1] in VOTE_CAST \
                            and row[member_state_2] in VOTE_CAST:
                        votings_together[member_state_1][member_state_2] = \
                            votings_together[member_state_1][member_state_2] + 1
                        if row[member_state_1] == row[member_state_2]:
                            same_votes[member_state_1][member_state_2] = same_votes[member_state_1][member_state_2] + 1
        votings_together.to_csv(same_votings_participated_path)
        same_votes.to_csv(same_votes_cast_path)
        return votings_together, same_votes


def calculate_same_vote_percentages_matrix(votings_together, same_votes):
    same_vote_percentages = pd.DataFrame(columns=ALL_MEMBER_STATE_CODES, index=ALL_MEMBER_STATE_CODES)
    for member_state_1 in ALL_MEMBER_STATE_CODES:
        for member_state_2 in ALL_MEMBER_STATE_CODES:
            if member_state_1 != member_state_2:
                same_vote_percentages[member_state_1][member_state_2] = \
                    same_votes[member_state_1][member_state_2] / votings_together[member_state_1][member_state_2] * 100
    return same_vote_percentages


def calculate_group_vote_data(votes_by_member_states):
    groups = [
        ('V4', ['SK', 'PL', 'HU', 'CZ']),
        ('Benelux', ['BE', 'NL', 'LU']),
        ('Baltic', ['EE', 'LT', 'LV']),
        ('Nordic', ['DK', 'SE', 'FI']),
        ('E3', ['DE', 'FR', 'UK']),
        ('Nordic-Baltic Six', ['EE', 'LT', 'LV', 'DK', 'SE', 'FI']),
        ('Weimar Triangle', ['DE', 'FR', 'PL']),
    ]
    group_names = [group[0] for group in groups]
    group_vote_data = pd.DataFrame(
        columns=['votings_participated_together', 'same_vote', 'same_vote_percentage'],
        index=group_names,
        data=0
    )
    for _, row in votes_by_member_states.iterrows():
        for group_name, members in groups:
            group_member_votes = [row[member] for member in members]
            if set(group_member_votes).issubset(VOTE_CAST):
                group_vote_data['votings_participated_together'][group_name] = \
                    group_vote_data['votings_participated_together'][group_name] + 1.0
                if all(group_member_vote == group_member_votes[0] for group_member_vote in group_member_votes):
                    group_vote_data['same_vote'][group_name] = group_vote_data['same_vote'][group_name] + 1.0
    for group_name in group_names:
        group_vote_data['same_vote_percentage'][group_name] = \
            group_vote_data['same_vote'][group_name] / \
            group_vote_data['votings_participated_together'][group_name] * 100
    return group_vote_data


def calculate_before_after(votes_by_member_states, dividing_date):
    member_state_pairings_with_date = [
        (['SK', 'HU'], '2020-02-29', 'HU-SK - SMER'),
        (['CZ', 'HU'], '2021-10-09', 'HU-CZ - Babis'),
    ]
    group_names = [
        member_state_pairing[2] for member_state_pairing in member_state_pairings_with_date
    ]
    group_vote_data_before = pd.DataFrame(
        columns=['votings_participated_together', 'same_vote', 'same_vote_percentage'],
        index=group_names,
        data=0
    )
    group_vote_data_after = pd.DataFrame(
        columns=['votings_participated_together', 'same_vote', 'same_vote_percentage'],
        index=group_names,
        data=0
    )
    for _, row in votes_by_member_states.iterrows():
        for member_state_pairing, date, group_name in member_state_pairings_with_date:
            if row[member_state_pairing[0]] in VOTE_CAST and row[member_state_pairing[1]] in VOTE_CAST:
                vote_date = datetime.strptime(row['date'], '%Y-%m-%d')
                data_to_add = group_vote_data_before if vote_date < dividing_date else group_vote_data_after
                data_to_add['votings_participated_together'][group_name] = \
                    data_to_add['votings_participated_together'][group_name] + 1.0
                if row[member_state_pairing[0]] == row[member_state_pairing[0]]:
                    data_to_add['same_vote'][group_name] = data_to_add['same_vote'][group_name] + 1.0
    for member_states in member_state_pairings_with_date:
        member_states['same_vote_percentage'][group_name] = \
            member_states['same_vote'][group_name] / \
            member_states['votings_participated_together'][group_name] * 100
    return member_states


def merge_voting_data(votings, voting_results):
    enriched_voting_data = []
    for voting_result in voting_results:
        vote_process_url = voting_result['voteProc']['value']
        voting_data_results = [voting for voting in votings if voting['voteProc']['value'] == vote_process_url]
        # TODO check sparql query to eliminate this part
        assert len(
            set([voting_data_result['decisionDate']['value'] for voting_data_result in voting_data_results])
        ) == 1
        voting_data = voting_data_results[0]
        enriched_voting_data.append({**voting_result, 'decisionDate': voting_data['decisionDate']['value']})
    return enriched_voting_data


if __name__ == "__main__":

    voting_results = fetch_query_results(SPARQL_QUERY_FOR_ALL_VOTE_RESULTS)

    votings = fetch_query_results(SPARQL_QUERY_FOR_ALL_VOTES)

    full_voting_data = merge_voting_data(votings, voting_results)

    votes_by_member_states = get_cacheable_data(
        VOTES_BY_MEMBER_STATES_FILENAME,
        calculate_votes_by_member_states,
        full_voting_data,
    )

    votings_together, same_votes = get_processed_vote_data(votes_by_member_states)

    same_vote_percentages = get_cacheable_data(
        SAME_VOTE_PERCENTAGES_FILENAME,
        calculate_same_vote_percentages_matrix,
        votings_together,
        same_votes,
    )

    get_cacheable_data(
        GROUP_VOTE_DATA_FILENAME,
        calculate_group_vote_data,
        votes_by_member_states,
    )

    get_cacheable_data(
        'test.csv',
        calculate_before_after,
        votes_by_member_states,
        datetime.strptime('2020-02-29', '%Y-%m-%d')
    )

    print(same_vote_percentages)
