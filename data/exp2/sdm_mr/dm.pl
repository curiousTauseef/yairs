#!/usr/bin/perl

# example usage
print "1: ";
print formulate_query( "obama family tree", "sd", 0.8, 0.1, 0.1) . "\n";
print "2: ";
print formulate_query( "french lick resort casino", "sd", 0.8, 0.1, 0.1) . "\n";
print "6: ";
print formulate_query( "kcs", "sd", 0.8, 0.1, 0.1) . "\n";
print "7: ";
print formulate_query( "air travel information", "sd", 0.8, 0.1, 0.1) . "\n";
print "15: ";
print formulate_query( "espn sports", "sd", 0.8, 0.1, 0.1) . "\n";
print "16: ";
print formulate_query( "arizona game fish", "sd", 0.8, 0.1, 0.1) . "\n";
print "17: ";
print formulate_query( "poker tournaments", "sd", 0.8, 0.1, 0.1) . "\n";
print "18: ";
print formulate_query( "wedding budget calculator", "sd", 0.8, 0.1, 0.1) . "\n";
print "21: ";
print formulate_query( "volvo", "sd", 0.8, 0.1, 0.1) . "\n";
print "25: ";
print formulate_query( "euclid", "sd", 0.8, 0.1, 0.1) . "\n";
print "26: ";
print formulate_query( "lower heart rate", "sd", 0.8, 0.1, 0.1) . "\n";
print "29: ";
print formulate_query( "ps 2 games", "sd", 0.8, 0.1, 0.1) . "\n";
print "31: ";
print formulate_query( "atari", "sd", 0.8, 0.1, 0.1) . "\n";
print "32: ";
print formulate_query( "website design hosting", "sd", 0.8, 0.1, 0.1) . "\n";
print "36: ";
print formulate_query( "gps", "sd", 0.8, 0.1, 0.1) . "\n";
print "37: ";
print formulate_query( "pampered chef", "sd", 0.8, 0.1, 0.1) . "\n";
print "38: ";
print formulate_query( "dogs adoption", "sd", 0.8, 0.1, 0.1) . "\n";
print "39: ";
print formulate_query( "disneyland hotel", "sd", 0.8, 0.1, 0.1) . "\n";
print "41: ";
print formulate_query( "orange county convention center", "sd", 0.8, 0.1, 0.1) . "\n";
print "42: ";
print formulate_query( "music man", "sd", 0.8, 0.1, 0.1) . "\n";
print "46: ";
print formulate_query( "alexian brothers hospital", "sd", 0.8, 0.1, 0.1) . "\n";
print "54: ";
print formulate_query( "president united states", "sd", 0.8, 0.1, 0.1) . "\n";
print "56: ";
print formulate_query( "uss yorktown charleston sc", "sd", 0.8, 0.1, 0.1) . "\n";
print "59: ";
print formulate_query( "build fence", "sd", 0.8, 0.1, 0.1) . "\n";
print "62: ";
print formulate_query( "texas border patrol", "sd", 0.8, 0.1, 0.1) . "\n";
print "73: ";
print formulate_query( "neil young", "sd", 0.8, 0.1, 0.1) . "\n";
print "80: ";
print formulate_query( "keyboard reviews", "sd", 0.8, 0.1, 0.1) . "\n";
print "82: ";
print formulate_query( "joints", "sd", 0.8, 0.1, 0.1) . "\n";
print "91: ";
print formulate_query( "er tv show", "sd", 0.8, 0.1, 0.1) . "\n";
print "97: ";
print formulate_query( "south africa", "sd", 0.8, 0.1, 0.1) . "\n";



#
# formulates a query based on query text and feature weights
#
# arguments:
#    * query - string containing original query terms separated by spaces
#    * type  - string. "sd" for sequential dependence or "fd" for full dependence variant. defaults to "fd".
#    * wt[0] - weight assigned to term features
#    * wt[1] - weight assigned to ordered (#1) features
#    * wt[2] - weight assigned to unordered (#uw) features
#
sub formulate_query {
    my ( $q, $type, @wt ) = @_;

    # trim whitespace from beginning and end of query string
    $q =~ s/^\s+|\s+$//g;
    
    my $queryT = "#and( ";
    my $queryO = "#and(";
    my $queryU = "#and(";
    
    # generate term features (f_T)
    my @terms = split(/\s+/ , $q);
    my $term;
    foreach $term ( @terms ) {
	$queryT .= "$term ";
    }

    my $num_terms = @terms;
    
    # skip the rest of the processing if we're just
    # interested in term features or if we only have 1 term
    if( ( $wt[1] == 0.0 && $wt[2] == 0.0 ) || $num_terms == 1 ) {
	return $queryT . ")";
    }
    
    # generate the rest of the features
    my $start = 1;
    if( $type eq "sd" ) { $start = 3; }
    for( my $i = $start ; $i < 2 ** $num_terms ; $i++ ) {
	my $bin = unpack("B*", pack("N", $i)); # create binary representation of i
	my $num_extracted = 0;
	my $extracted_terms = "";

	# get query terms corresponding to 'on' bits
	for( my $j = 0 ; $j < $num_terms ; $j++ ) {
	    my $bit = substr($bin, $j - $num_terms, 1);
	    if( $bit eq "1" ) {
		$extracted_terms .= "$terms[$j] ";
		$num_extracted++;
	    }
	}
	
	if( $num_extracted == 1 ) { next; } # skip these, since we already took care of the term features...
	if( $bin =~ /^0+11+[^1]*$/ ) { # words in contiguous phrase, ordered features (f_O)
	    $queryO .= " #near/1( $extracted_terms) ";
	}
	$queryU .= " #uw/" . 3*$num_extracted . "( $extracted_terms) "; # every subset of terms, unordered features (f_U)
	if( $type eq "sd" ) { $i *= 2; $i--; }
    }

    my $query = "#weight(";
    if( $wt[0] != 0.0 && $queryT ne "#and( " ) { $query .= " $wt[0] $queryT)"; }
    if( $wt[1] != 0.0 && $queryO ne "#and(" ) { $query .= " $wt[1] $queryO)"; }
    if( $wt[2] != 0.0 && $queryU ne "#and(" ) { $query .= " $wt[2] $queryU)"; }

    if( $query eq "#weight(" ) { return ""; } # return "" if we couldn't formulate anything
    
    return $query . " )";
}
