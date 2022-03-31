import requests
import pandas as pd

# copied from query at:
# https://www.consilium.europa.eu/en/general-secretariat/corporate-policies/transparency/open-data/voting-results/
SPARQL_QUERY_FOR_ALL_VOTES = "PREFIX acts: <http://data.consilium.europa.eu/id/acts/>  PREFIX tax: <http://data.consilium.europa.eu/id/taxonomy/>  PREFIX codi: <http://data.consilium.europa.eu/def/codi/>  PREFIX skos:<http://www.w3.org/2004/02/skos/core#>  PREFIX dct: <http://purl.org/dc/terms/>  PREFIX foaf: <http://xmlns.com/foaf/0.1/>  PREFIX owl: <http://www.w3.org/2002/07/owl#>  PREFIX votpos: <http://data.consilium.europa.eu/id/taxonomy/votingposition>  SELECT distinct ?voteProc ?votingRuleId ?policyAreaId ?voteOnDocumentNumber ?votingInstCode ?legisProcId ?decisionDate  group_concat(distinct ?councilActionId;separator='|') as ?councilActionIdGrouped ?meetingSessionNumber ?councilConfigurationId  ?docExpression ?docTitle ?registerPage ?actNumber ?actTypeId  FROM <http://data.consilium.europa.eu/id/dataset/VotingResults>  FROM <http://data.consilium.europa.eu/id/dataset/PublicRegister>  WHERE {    ?voteProc a codi:VotingProcedure.      ?voteProc codi:votingRule ?votingRule.      ?votingRule skos:notation ?votingRuleId    optional {      ?voteProc codi:policyArea ?policyArea.      ?policyArea skos:notation ?policyAreaId    }.    optional {       ?voteProc codi:voteOn ?voteOn.      FILTER (CONTAINS(STR(?voteOn), 'INIT')).      ?voteOn codi:document_number ?voteOnDocumentNumber.      optional { ?voteOn codi:act_number ?actNumber. }.      optional {        ?voteOn codi:actType ?actType.        ?actType skos:notation ?actTypeId.      }.      optional { ?voteOn foaf:page ?registerPage. }      ?voteOn codi:expressed ?docExpression.      ?docExpression dct:title ?docTitle.      ?docExpression dct:language ?docLanguage.      FILTER ( lang(?docTitle) = 'en' || lang(?docTitle) = 'en' )    }.    optional { ?voteProc codi:forInterInstitutionalCode ?votingInstCode }.    optional {      ?voteProc codi:legislativeProcedure ?legisProc.      ?legisProc skos:notation ?legisProcId.    }.    ?voteProc codi:hasVoteOutcome ?voteDecision.    ?voteDecision dct:dateAccepted ?decisionDate.    optional {      ?voteDecision codi:councilAction ?councilAction.      ?councilAction skos:notation ?councilActionId.    }.    optional {      ?meeting codi:appliesProcedure ?voteProc.      optional { ?meeting codi:meetingsessionnumber ?meetingSessionNumber. }      optional {        ?meeting codi:configuration ?meetingConfig.        ?meetingConfig a skos:Concept.        ?meetingConfig skos:notation ?councilConfigurationId.      }    }  } ORDER BY DESC(?decisionDate), ?votingInstCode"
SPARQL_QUERY_FOR_ALL_VOTE_RESULTS = "PREFIX acts: <http://data.consilium.europa.eu/id/acts/>  PREFIX tax: <http://data.consilium.europa.eu/id/taxonomy/>  PREFIX codi: <http://data.consilium.europa.eu/def/codi/>  PREFIX skos:<http://www.w3.org/2004/02/skos/core#>  PREFIX dct: <http://purl.org/dc/terms/>  PREFIX foaf: <http://xmlns.com/foaf/0.1/>  PREFIX owl: <http://www.w3.org/2002/07/owl#>  PREFIX votpos: <http://data.consilium.europa.eu/id/taxonomy/votingposition>  SELECT distinct ?voteProc ?voteDecision  group_concat(distinct ?countryCodeInFavour;separator='|') as ?countryCodeInFavourGrouped  group_concat(distinct ?countryCodeAgainst;separator='|') as ?countryCodeAgainstGrouped  group_concat(distinct ?countryCodeAbstained;separator='|') as ?countryCodeAbstainedGrouped  group_concat(distinct ?countryCodeNotParticipating;separator='|') as ?countryCodeNotParticipatingGrouped  FROM <http://data.consilium.europa.eu/id/dataset/VotingResults>  WHERE {     ?voteProc a codi:VotingProcedure.     ?voteProc codi:hasVoteOutcome ?voteDecision.     ?voteDecision codi:hasVotingPosition ?countryVote_uri .     optional {        ?countryVote_uri codi:votingposition <http://data.consilium.europa.eu/id/taxonomy/votingposition/votedagainst>.        ?countryVote_uri codi:country ?countryVoteAgainst_uri.        ?countryVoteAgainst_uri skos:notation ?countryCodeAgainst.     }     optional {        ?countryVote_uri codi:votingposition <http://data.consilium.europa.eu/id/taxonomy/votingposition/votedinfavour>.        ?countryVote_uri codi:country ?countryVoteInFavour_uri.        ?countryVoteInFavour_uri skos:notation ?countryCodeInFavour.     }     optional {        ?countryVote_uri codi:votingposition <http://data.consilium.europa.eu/id/taxonomy/votingposition/abstained>.        ?countryVote_uri codi:country ?countryAbstained_uri.        ?countryAbstained_uri skos:notation ?countryCodeAbstained.     }     optional {        ?countryVote_uri codi:votingposition <http://data.consilium.europa.eu/id/taxonomy/votingposition/notparticipating>.        ?countryVote_uri codi:country ?countryNotParticipating_uri.        ?countryNotParticipating_uri skos:notation ?countryCodeNotParticipating.     }  } "
VOTE_VALUES_BY_ALL_VOTE_RESULTS_KEYS = {
    "countryCodeInFavourGrouped": "Y",
    "countryCodeAgainstGrouped": "N",
    "countryCodeAbstainedGrouped": "A",
    "countryCodeNotParticipatingGrouped": "0",
}


def fetch_votings():
    response = requests.post("https://data.consilium.europa.eu/sparql", data={
        "query": SPARQL_QUERY_FOR_ALL_VOTE_RESULTS,
        "format": "application/sparql-results+json",
        "timeout": "0"

    })
    return response.json()['results']['bindings']


if __name__ == "__main__":

    votings = fetch_votings()

    votes_by_member_states = pd.DataFrame(
        columns=[
            'AT', 'BE', 'BG', 'CY', 'CZ', 'DE', 'DK', 'EE', 'EL', 'ES', 'FI', 'FR', 'HU', 'IE', 'IT', 'LT', 'LU',
            'LV', 'MT', 'NL', 'PL', 'PT', 'RO', 'SE', 'SI', 'SK', 'UK'
        ]
    )
    for voting in votings:
        row_data = {}
        for vote_key in VOTE_VALUES_BY_ALL_VOTE_RESULTS_KEYS.keys():
            vote_value = VOTE_VALUES_BY_ALL_VOTE_RESULTS_KEYS[vote_key]
            voting_countries_unparsed = voting[vote_key]["value"]
            voting_member_states = voting_countries_unparsed.split("|")
            for member_state in voting_member_states:
                if member_state != '':
                    row_data[member_state] = vote_value
        votes_by_member_states = votes_by_member_states.append(row_data, ignore_index=True)
    print(votes_by_member_states)
