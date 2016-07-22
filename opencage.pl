#!/usr/bin/perl
use strict;
use warnings FATAL => 'all';

use 5.012;
use utf8;

use Getopt::Std;
use Term::ReadKey;
# use DateTime;
# use DateTime::TimeZone;
use FileHandle;
use File::Basename;

use Data::Dumper;
use Geo::Coder::OpenCage;
use URI::Escape;

use Math::Trig;

use List::Util qw(first all);
use List::MoreUtils qw(uniq);

use DBI;
use LPG::prompt qw(prompt_init prompt_for_value prompt_menu);
use LPG::SQLO;
use LPG::SQLO_prompt qw(sql_file_check);
use LPG::SQL_SOCIAL;



our ($opt_h, $opt_d, $opt_v, $opt_n, $opt_g, $opt_u, $opt_r, $opt_R, $opt_S, $opt_A, $opt_f);

# -h                   :: print usage information.
# -d                   :: enable debugging output.
# -v                   :: enable verbose output.
# -A                   :: Pull ALL dealers
# -f                   :: force a lookup even if Geo data present
# -g #.##              :: Geo distance threashold.  If old coordinates off by more than #.## Km, then update coordinates.
# -n [ DEV | QA | QA_SLAVE | LIVE | LIVE_SLAVE ] :: select a nexus database to use.
# -u <update_SQL_file> :: specify the name of the SQL update file.
# -r <revert_SQL_file> :: specify the name of the SQL revert file.
# -R <#>               :: randomly select # dealers
# -S <#>               :: sleep for # seconds between accounts

getopts( 'hdvAfn:g:u:r:R:S:' );

$| = 1;

my ($rfh, $ufh);
my $source_tablename = 'temp_active_accounts';
my $DEFAULT_UPDATE_FILENAME_FORMAT = "opencage_update_%s.sql";
my $DEFAULT_REVERT_FILENAME_FORMAT = "opencage_revert_%s.sql";

my ($NORM, $BOLD, $DIM, $BLACK, $HIBLACK, $RED, $HIRED, $YELLOW, $HIYELLOW, $LOYELLOW, $GREEN, $HIGREEN, $LOGREEN, $BLUE, $HIBLUE, $MAGENTA, $HIMAGENTA, $CYAN, $HICYAN, $WHITE);
my %valid_US_states = ("AL" => 'S', "AK" => 'S', "AZ" => 'S', "AR" => 'S', "CA" => 'S', "CO" => 'S', "CT" => 'S', "DE" => 'S', "FL" => 'S', "GA" => 'S',
    "HI" => 'S', "ID" => 'S', "IL" => 'S', "IN" => 'S', "IA" => 'S', "KS" => 'S', "KY" => 'S', "LA" => 'S', "ME" => 'S', "MD" => 'S',
    "MA" => 'S', "MI" => 'S', "MN" => 'S', "MS" => 'S', "MO" => 'S', "MT" => 'S', "NE" => 'S', "NV" => 'S', "NH" => 'S', "NJ" => 'S',
    "NM" => 'S', "NY" => 'S', "NC" => 'S', "ND" => 'S', "OH" => 'S', "OK" => 'S', "OR" => 'S', "PA" => 'S', "RI" => 'S', "SC" => 'S',
    "SD" => 'S', "TN" => 'S', "TX" => 'S', "UT" => 'S', "VT" => 'S', "VA" => 'S', "WA" => 'S', "WV" => 'S', "WI" => 'S', "WY" => 'S' );

my %US_states = (
    "ALABAMA" => "AL", "ALASKA" => "AK", "ARIZONA" => "AZ", "ARKANSAS" => "AR", "CALIFORNIA" => "CA",
    "COLORADO" => "CO", "CONNECTICUT" => "CT", "DELAWARE" => "DE", "FLORIDA" => "FL", "GEORGIA" => "GA",
    "HAWAII" => "HI", "IDAHO" => "ID", "ILLINOIS" => "IL", "INDIANA" => "IN", "IOWA" => "IA",
    "KANSAS" => "KS", "KENTUCKY" => "KY", "LOUISIANA" => "LA", "MAINE" => "ME", "MARYLAND" => "MD",
    "MASSACHUSETTS" => "MA", "MICHIGAN" => "MI", "MINNESOTA" => "MN", "MISSISSIPPI" => "MS", "MISSOURI" => "MO",
    "MONTANA" => "MT", "NEBRASKA" => "NE", "NEVADA" => "NV", "NEW HAMPSHIRE" => "NH", "NEW JERSEY" => "NJ",
    "NEW MEXICO" => "NM", "NEW YORK" => "NY", "NORTH CAROLINA" => "NC", "NORTH DAKOTA" => "ND", "OHIO" => "OH",
    "OKLAHOMA" => "OK", "OREGON" => "OR", "PENNSYLVANIA" => "PA", "RHODE ISLAND" => "RI", "SOUTH CAROLINA" => "SC",
    "SOUTH DAKOTA" => "SD", "TENNESSEE" => "TN", "TEXAS" => "TX", "UTAH" => "UT", "VERMONT" => "VT",
    "VIRGINIA" => "VA", "WASHINGTON" => "WA", "WEST VIRGINIA" => "WV", "WISCONSIN" => "WI", "WYOMING" => "WY" );


if (index($ENV{TERM},"color") != -1) {
    $NORM="\033[0m";
    $BOLD="\033[1m";
    $DIM="\033[2m";
    $BLACK="\033[0;30m";
    $HIBLACK="\033[1;30m";
    $RED="\033[0;31m";
    $HIRED="\033[1;31m";
    $GREEN="\033[0;32m";
    $HIGREEN="\033[1;32m";
    $LOGREEN="\033[2;32m";
    $YELLOW="\033[0;33m";
    $HIYELLOW="\033[1;33m";
    $LOYELLOW="\033[2;33m";
    $BLUE="\033[0;34m";
    $HIBLUE="\033[1;34m";
    $MAGENTA="\033[0;35m";
    $HIMAGENTA="\033[1;35m";
    $CYAN="\033[0;36m";
    $HICYAN="\033[1;36m";
    $WHITE = "\033[0;37m";
};


#print $BLACK, "TEST CONTENT (BLACK)\n";
#print $HIBLACK, "TEST CONTENT (HIBLACK)\n";
#print $RED, "TEST CONTENT (RED)\n";
#print $HIRED, "TEST CONTENT (HIRED)\n";
#print $GREEN, "TEST CONTENT (GREEN)\n";
#print $HIGREEN, "TEST CONTENT (HIGREEN)\n";
#print $YELLOW, "TEST CONTENT (YELLOW)\n";
#print $HIYELLOW, "TEST CONTENT (HIYELLOW)\n";
#print $BLUE, "TEST CONTENT (BLUE)\n";
#print $HIBLUE, "TEST CONTENT (HIBLUE)\n";
#print $MAGENTA, "TEST CONTENT (MAGENTA)\n";
#print $HIMAGENTA, "TEST CONTENT (HIMAGENTA)\n";
#print $CYAN, "TEST CONTENT (CYAN)\n";
#print $HICYAN, "TEST CONTENT (HICYAN)\n", $NORM;
#



if ($opt_h) {
    usage();
    exit;
}

if (!defined( $opt_n )) {
    usage();
    exit;
}

if ($opt_d && $opt_v) {
    $LPG::SQLO::Debug = 1;
}


my $sleep_target = undef;
set_sleep_target();

prompt_init();

########################## SQL UPDATE FILE CHECK/SELECTION ##########################
if (!defined $opt_u) {
    $opt_u = sprintf( $DEFAULT_UPDATE_FILENAME_FORMAT, $opt_n );
    printf( "No update SQL file specified, using default '%s'\n", $opt_u ) if ($opt_d);
}
sql_file_check($opt_u);

########################## SQL REVERT FILE CHECK/SELECTION ##########################
if (!defined $opt_r) {
    $opt_r = sprintf( $DEFAULT_REVERT_FILENAME_FORMAT, $opt_n );
    printf( "No revert SQL file specified, using default '%s'\n", $opt_r ) if ($opt_d);
}
sql_file_check($opt_r);


########################## DATABASE CONNECTION INIT ##########################

# Create a connection to the Social DB
my $dbo = LPG::SQLO->new($LPG::SQL_SOCIAL::social_db_normal_site_names{$opt_n}, \%LPG::SQL_SOCIAL::social_db_definitions);

if (!defined $dbo || !defined $dbo->{status}) {
    if (defined $dbo->{error}) {
        print STDERR $dbo->{error}, "\n";
    } else {
        print STDERR "Failed to create DB object.\n";
    }
    usage();
    exit;
}

my $connection = $dbo->connect_to_db();
if (!defined( $connection )) {
    print STDERR "Failed to connect to the database.\n";
    exit;
}

my $sth = $connection->column_info( undef, undef, $source_tablename, undef );
my $temp_active_dealers_columns = $sth->fetchall_hashref( 'COLUMN_NAME' );


########################## GENERATE DATABASE QUERY ##########################
my $query = "";
if ($opt_A || $opt_R) {
    $query = sprintf("select * from %s;", $source_tablename);
    if (scalar @ARGV > 0) {
        print STDERR $YELLOW, "WARNING: Extra arguments being ignored because ", $WHITE, ($opt_A ? "-A" : "-R"), $YELLOW
            ,
            " argument present. (", $WHITE, join( " ", @ARGV ), $YELLOW, ")\n", $NORM;
    }
}
elsif (scalar @ARGV > 0) {
    my @quoted_dealer_ids = map { $connection->quote( $_,
        $temp_active_dealers_columns->{dealer_id}->{DATA_TYPE} ); } @ARGV;
    $query = sprintf( "select * from %s where dealer_id in (%s);", $source_tablename, join( ",", @quoted_dealer_ids ) );
}
else {
    print STDERR $RED, "Nothing to do.\n", $NORM;
    exit;
}

########################## RUN DATABASE QUERY ###########################
print $CYAN, "Database query: ", $NORM, $query, "\n" if $opt_d;
my ($rows, $info) = $dbo->sql_select( $query );
my $num_rows = @$rows;



if ($opt_R) {
    # randomly select a dealer (get rid of the rest)
    my @picks = ();
    while ($opt_R > 0) {
        my $pick = int( rand( $num_rows ) );
        my $account = ${$rows}[$pick];

        if ($opt_f || !defined( $account->{timeZoneId} ) || $account->{timeZoneId} eq "") {
            push @picks, $account;
            $opt_R--;
        }
    }

    $rows = \@picks;
    $num_rows = scalar @$rows;
    printf( "Randomly selected %d dealer%s\n", $num_rows, ($num_rows == 1 ? "" : "s"));
}

if (!$num_rows) {
    print STDERR "No rows found.\n";
    exit;
}


##################### transform to uid key'd map #####################
my %accounts = ();
foreach my $row (sort { $a->{dealer_id} cmp $b->{dealer_id} } @$rows) {
    $accounts{$row->{dealer_id}} = $row;
}


################# CATEGORIZE the ACCOUNTS #####################
my @no_geo_location = ();
my @no_location_at_all = ();
my @no_address = ();
foreach my $account_id (sort keys %accounts) {
    my $account = $accounts{$account_id};

    if (hasGeoCoordinates($account)) {
        # latitude and longitude present
        # do they have an address on file too?
        if (!hasAddress($account)) {
            # no address,
            push @no_address, $account_id;
        }
    }
    else {
        # no geographic coordinates
        if (hasAddress($account)) {
            push @no_geo_location, $account_id;
        }
        else {
            push @no_location_at_all, $account_id;
        }
    }
}


###############  NOT ENOUGH ADDRESS and NO LAT/LON, so Remove from the accounts list  ###############
if (scalar @no_location_at_all > 0) {
    printf( "%sThe following %d dealers have no known address nor geographic coordinates.\n%s", $RED,
        scalar @no_location_at_all, $NORM );

    foreach my $account_id (sort @no_location_at_all) {
        my $account = $accounts{$account_id};

        print_dealer($account);

        # remove it from the accounts collection, there's nothing we can do.
        delete $accounts{$account_id};
    }
    print "\n";
}

if (scalar @no_address > 0) {
    printf( "%sThe following %d dealers have no address recorded...\n%s", $RED,
        scalar @no_address, $NORM );

    foreach my $account_id (sort @no_address) {
        my $account = $accounts{$account_id};

        print_dealer($account);
    }
    print "\n";
}

if (scalar @no_geo_location > 0) {
    printf( "%sThe following %d dealers have no geographic coordinates recorded...\n%s", $RED,
        scalar @no_geo_location, $NORM);

    foreach my $account_id (sort @no_geo_location) {
        my $account = $accounts{$account_id};

        print_dealer($account);
    }
    print "\n";
}


my $Geocoder = Geo::Coder::OpenCage->new(
    api_key => 'HIDDEN_ON_PURPOSE',
);

my $counter = 0;
printf( "%d dealers\n", scalar keys %accounts );
print $RED, "****************************************************************************\n", $NORM;
foreach my $account_id (sort keys %accounts) {
    my $account = $accounts{$account_id};
    $counter++;

    printf("%s%d%s%s <<<<<<<<  BEFORE PROCESSING ::%s ", $HIGREEN, $counter, $NORM, $LOGREEN, $NORM);
    print_dealer($account);

    # Try to get the addresses we're missing  (save the timezone info if present)
    if (first { $_ eq $account_id } @no_address) {

        my $geo = $Geocoder->reverse_geocode(lat => $account->{latitude}, lng => $account->{longitude});

        if (defined $geo) {
            if ($opt_d) {
                print "REVERSE GEOCODE:";
                print Dumper $geo;
            }

            my $fav_result = opencage_select_result($geo);
            if (defined $fav_result) {

                my $address = opencage_decode_address( $fav_result );

                if (defined $address) {
                    # Save ORIG state for possible REVERT
                    $account->{ORIG}->{address1} = $account->{address1};
                    $account->{ORIG}->{city} = $account->{city};
                    $account->{ORIG}->{state} = $account->{state};
                    $account->{ORIG}->{postalcode} = $account->{postalcode};
                    $account->{ORIG}->{country} = $account->{country};
                    # Update Address
                    $account->{address1} = $address->{address1};
                    $account->{city} = $address->{city};
                    $account->{state} = $address->{state};
                    $account->{postalcode} = $address->{postalcode};
                    $account->{country} = $address->{country};

                    push @{$account->{updated}}, "address";
                }

                print "Checking timezone...\n" if $opt_d;
                my $tz = opencage_decode_timezone( $fav_result );

                if ($opt_f || !defined $account->{timeZone} ) {
                    if (defined $tz->{timeZoneOffset}) {
                        printf("  setting timezone offset: %d\n", $tz->{timeZoneOffset}) if $opt_d;
                        $account->{ORIG}->{timeZone} = $account->{timeZone};
                        $account->{timeZone} = $tz->{timeZoneOffset};

                        push @{$account->{updated}}, "timezone";
                    }
                }

                if ($opt_f || !defined $account->{timeZoneId}) {
                    if (defined $tz->{timeZoneName}) {
                        printf("  setting timezone name: %s\n", $tz->{timeZoneName}) if $opt_d;
                        $account->{ORIG}->{timeZoneId} = $account->{timeZoneId};
                        $account->{timeZoneId} = $tz->{timeZoneName};

                        push @{$account->{updated}}, "timezonename";
                    }
                }
            }
        }
    }


    # using the address, verify the latitude and longitude and save the timezone
    my $geo = $Geocoder->geocode(location => format_address($account));

    if (defined $geo) {
        if ($opt_d) {
            print "FORWARD GEOCODE:";
            print Dumper $geo;
        }

        my $fav_result = opencage_select_result($geo);
        if (defined $fav_result) {

            print "Checking coordinates...\n" if $opt_d;
            my $latlng = opencage_decode_geometry( $fav_result );

            if (defined $latlng) {
                my $distance = distance($account->{latitude} || 0.00, $account->{longitude} || 0.00, $latlng->{latitude}, $latlng->{longitude});

                printf("coordinate comparison:  db vs api = %s (%9.5f)\n", sprintf_distance($distance), $distance) if $opt_d;

                if ($opt_g ||  !defined $account->{latitude} || !defined $account->{longitude}) {
                    if ((defined $opt_g && $distance > $opt_g) || !defined $account->{latitude} || !defined $account->{longitude}) {
                        # Save ORIG in case of REVERT
                        $account->{ORIG}->{latitude} = $account->{latitude};
                        $account->{ORIG}->{longitude} = $account->{longitude};
                        if (defined $account->{latitude} && defined $account->{longitude}) {
                            # new coord. too far from old coord., so update lat and lng
                            printf( "%sDB vs API coordinate distance %s is greater than %s. Updating coordinates.%s\n",
                                $CYAN, sprintf_distance( $opt_g ), sprintf_distance( $distance ), $NORM);
                        }
                        $account->{latitude}  = $latlng->{latitude};
                        $account->{longitude} = $latlng->{longitude};

                        push @{$account->{updated}}, "geometry";
                    }
                }
            }

            print "Checking timezone...\n" if $opt_d;
            my $tz = opencage_decode_timezone( $fav_result );

            if ($opt_f || !defined $account->{timeZone} ) {
                if (defined $tz->{timeZoneOffset}) {
                    printf("  setting timezone offset: %d\n", $tz->{timeZoneOffset}) if $opt_d;
                    $account->{ORIG}->{timeZone} = $account->{timeZone};
                    $account->{timeZone} = $tz->{timeZoneOffset};

                    push @{$account->{updated}}, "timezone";
                }
            }

            if ($opt_f || !defined $account->{timeZoneId}) {
                if (defined $tz->{timeZoneName}) {
                    printf("  setting timezone name: %s\n", $tz->{timeZoneName}) if $opt_d;
                    $account->{ORIG}->{timeZoneId} = $account->{timeZoneId};
                    $account->{timeZoneId} = $tz->{timeZoneName};

                    push @{$account->{updated}}, "timezonename";
                }
            }
        }
        else {
            print $RED, "Address not found.\n", $NORM;
        }
    }

    if (defined $account->{updated} && scalar @{$account->{updated}}) {
        print $BLUE, join( ", ", @{$account->{updated}} ), ": Updated\n", $NORM;
    }
    printf("%s%d%s%s >>>>>>>>>  AFTER PROCESSING ::%s ", $HIGREEN, $counter, $NORM, $LOGREEN, $NORM);
    print_dealer($account);


    if ($opt_r) {
        $rfh = FileHandle->new( $opt_r, "a" );
        if (!defined $rfh) {
            printf( STDERR "Failed to open '%s' for append. (%s%s%s)\n", $opt_r, $RED, $!, $NORM );
        }
    }
    if ($opt_u) {
        $ufh = FileHandle->new( $opt_u, "a" );
        if (!defined $ufh) {
            printf( STDERR "Failed to open '%s' for append. (%s%s%s)\n", $opt_u, $RED, $!, $NORM );
        }
    }

    generate_account_update_sql($account, $rfh, $ufh);

    if (defined $rfh) {
        $rfh->close;
    }
    if (defined $ufh) {
        $ufh->close;
    }


    my $sleep_time = int( rand( $sleep_target ) );
    printf( "%sSleeping for %d seconds.%s\n", $DIM, $sleep_time, $NORM );
    sleep $sleep_time;

}

exit 0;


sub generate_account_update_sql {
    my ($account, $revertfh, $updatefh) = @_;

    # address1, city, state, postalcode and country properties :: create the commands to revert and update
    if (first { $_ eq 'address' } @{$account->{updated}}) {
        my $updatgeometry = "update %s set address1=%s, city=%s, state=%s, postalcode=%s, country=%s  where dealer_id=%s;";
        my $revert_command = sprintf( $updatgeometry, $source_tablename,
            $connection->quote( $account->{ORIG}->{address1}, $info->{address1}->{DATA_TYPE} ),
            $connection->quote( $account->{ORIG}->{city}, $info->{city}->{DATA_TYPE} ),
            $connection->quote( $account->{ORIG}->{state}, $info->{state}->{DATA_TYPE} ),
            $connection->quote( $account->{ORIG}->{postalcode}, $info->{postalcode}->{DATA_TYPE} ),
            $connection->quote( $account->{ORIG}->{country}, $info->{country}->{DATA_TYPE} ),
            $connection->quote( $account->{dealer_id}, $info->{dealer_id}->{DATA_TYPE} ) );

        my $update_command = sprintf( $updatgeometry, $source_tablename,
            $connection->quote( $account->{address1}, $info->{address1}->{DATA_TYPE} ),
            $connection->quote( $account->{city}, $info->{city}->{DATA_TYPE} ),
            $connection->quote( $account->{state}, $info->{state}->{DATA_TYPE} ),
            $connection->quote( $account->{postalcode}, $info->{postalcode}->{DATA_TYPE} ),
            $connection->quote( $account->{country}, $info->{country}->{DATA_TYPE} ),
            $connection->quote( $account->{dealer_id}, $info->{dealer_id}->{DATA_TYPE} ) );

        if (defined $revertfh) {
            $revertfh->printf( "%s\n", $revert_command );
        }
        if (defined $updatefh) {
            $updatefh->printf( "%s\n", $update_command );
        }
    }

    # latitude and longitude properties :: create the commands to revert and update
    if (first { $_ eq 'geometry' } @{$account->{updated}}) {
        my $updatgeometry = "update %s set latitude=%s, longitude=%s where dealer_id=%s;";
        my $revert_command = sprintf( $updatgeometry, $source_tablename,
            $connection->quote( $account->{ORIG}->{latitude}, $info->{latitude}->{DATA_TYPE} ),
            $connection->quote( $account->{ORIG}->{longitude}, $info->{longitude}->{DATA_TYPE} ),
            $connection->quote( $account->{dealer_id}, $info->{dealer_id}->{DATA_TYPE} ) );

        my $update_command = sprintf( $updatgeometry, $source_tablename,
            $connection->quote( $account->{latitude}, $info->{latitude}->{DATA_TYPE} ),
            $connection->quote( $account->{longitude}, $info->{longitude}->{DATA_TYPE} ),
            $connection->quote( $account->{dealer_id}, $info->{dealer_id}->{DATA_TYPE} ) );

        if (defined $revertfh) {
            $revertfh->printf( "%s\n", $revert_command );
        }
        if (defined $updatefh) {
            $updatefh->printf( "%s\n", $update_command );
        }
    }

    # timeZone property :: create the commands to revert and update
    if (first { $_ eq 'timezone' } @{$account->{updated}}) {
        my $update_timeZone = "update %s set timeZone=%s where dealer_id=%s;";
        my $old_timezone = ((defined $account->{ORIG}->{timeZone} && $account->{ORIG}->{timeZone} ne "NULL") ? sprintf( "%d", $account->{ORIG}->{timeZone}) : "NULL");
        my $revert_command = sprintf( $update_timeZone, $source_tablename,
            $connection->quote( $old_timezone, $info->{timeZone}->{DATA_TYPE} ),
            $connection->quote( $account->{dealer_id}, $info->{dealer_id}->{DATA_TYPE} ) );

        my $update_command = sprintf( $update_timeZone, $source_tablename,
            $connection->quote( sprintf( "%d", $account->{timeZone} ), $info->{timeZone}->{DATA_TYPE} ),
            $connection->quote( $account->{dealer_id}, $info->{dealer_id}->{DATA_TYPE} ) );

        if (defined $revertfh) {
            $revertfh->printf( "%s\n", $revert_command );
        }
        if (defined $updatefh) {
            $updatefh->printf( "%s\n", $update_command );
        }
    }

    # timeZoneId property :: create the commands to revert and update
    if (first { $_ eq 'timezonename' } @{$account->{updated}}) {
        my $update_timeZoneId = "update %s set timeZoneId=%s where dealer_id=%s;";
        my $revert_command = sprintf( $update_timeZoneId, $source_tablename,
            $connection->quote( $account->{ORIG}->{timeZoneId}, $info->{timeZoneId}->{DATA_TYPE} ),
            $connection->quote( $account->{dealer_id}, $info->{dealer_id}->{DATA_TYPE} ) );

        my $update_command = sprintf( $update_timeZoneId, $source_tablename,
            $connection->quote( $account->{timeZoneId}, $info->{timeZoneId}->{DATA_TYPE} ),
            $connection->quote( $account->{dealer_id}, $info->{dealer_id}->{DATA_TYPE} ) );

        if (defined $revertfh) {
            $revertfh->printf( "%s\n", $revert_command );
        }
        if (defined $updatefh) {
            $updatefh->printf( "%s\n", $update_command );
        }
    }
}

sub opencage_select_result {
    my ($oc_data) = @_;

    # select the "result" having the highest "confidence"
    my $max_confidence = -1;
    my $max_confidence_index = undef;

    print "Find the result having the highest confidence...\n" if $opt_d;
    for (my $idx = 0; $idx < scalar @{$oc_data->{results}}; $idx++)
    {
        my $result = ${$oc_data->{results}}[$idx];

        if ($opt_d) {
            print "======== CURRENT result ======";
            print Dumper $result;
            printf("Item %d:  Confidence: %d", $idx, $result->{confidence});
        }

        if ($result->{confidence} > $max_confidence) {
            $max_confidence = $result->{confidence};
            $max_confidence_index = $idx;
            print " (MAX sofar)\n" if $opt_d;
        }
        else {
            print "\n" if $opt_d;
        }
    }

    my $result_item = undef;
    if (defined $max_confidence_index) {
        $result_item = ${$oc_data->{results}}[$max_confidence_index];
    }

    $result_item;
}

sub opencage_decode_address {
    my ($result_item) = @_;
    my $address = undef;

    if (defined $result_item) {

        my $address_components = $result_item->{components};

        if ($opt_d) {
            print "Parsing address:\n";
            print Dumper $address_components;
        }

        if ($address_components->{_type} eq 'building') {
            my $street_address = undef;
            if (defined $address_components->{house_number} && defined $address_components->{road}) {
                $street_address = sprintf( "%s %s", $address_components->{house_number}, $address_components->{road});
            }
            elsif (defined $address_components->{building} && defined $address_components->{road}) {
                $street_address = sprintf( "%s, %s", $address_components->{road}, $address_components->{building});
            }
            elsif (defined $address_components->{road}) {
                $street_address = $address_components->{road};
            }

            $address = {
                "address1"   => $street_address,
                "city"       => $address_components->{town} || $address_components->{city} ,
                "state"      => (defined $US_states{uc( $address_components->{state} )} ? $US_states{uc( $address_components->{state} )} : $address_components->{state}),
                "country"    => uc $address_components->{country_code},
                "postalcode" => $address_components->{postcode}
            };
        }
        elsif ($address_components->{_type} eq 'road') {
            $address = {
                "address1"   => $address_components->{road},
                "city"       => $address_components->{town} || $address_components->{city} ,
                "state"      => (defined $US_states{uc( $address_components->{state} )} ? $US_states{uc( $address_components->{state} )} : $address_components->{state}),
                "country"    => uc $address_components->{country_code},
                "postalcode" => $address_components->{postcode}
            };
        }
        elsif ($address_components->{_type} eq 'city') {
            $address = {
                "city"       => $address_components->{town} || $address_components->{city} ,
                "state"      => (defined $US_states{uc( $address_components->{state} )} ? $US_states{uc( $address_components->{state} )} : $address_components->{state}),
                "country"    => uc $address_components->{country_code},
                "postalcode" => $address_components->{postcode}
            };
        }
        elsif ($address_components->{_type} eq 'county') {
            $address = {
                "state"      => (defined $US_states{uc( $address_components->{state} )} ? $US_states{uc( $address_components->{state} )} : $address_components->{state}),
                "country"    => uc $address_components->{country_code},
                "postalcode" => $address_components->{postcode}
            };
        }
        else {
            print $RED, "Unexpected type of address components in OpenCage response: ", $address_components->{_type}, $NORM, "\n";
        }
    }

    if ($opt_d) {
        print  "opencage_decode_address: returning:\n";
        printf( "   address1   = %s\n", $address->{address1} );
        printf( "   city       = %s\n", $address->{city} );
        printf( "   state      = %s\n", $address->{state} );
        printf( "   country    = %s\n", $address->{country} );
        printf( "   postalcode = %s\n", $address->{postalcode} );
    }

    $address;
}

sub opencage_decode_geometry {
    my ($result_item) = @_;
    my $geo = undef;

    my $latlng = $result_item->{geometry};
    if (defined $latlng) {
        $geo->{latitude}   = $latlng->{lat};
        $geo->{longitude} = $latlng->{lng};
    }

    if ($opt_d) {
        print  "opencage_decode_geometry: returning:\n";
        printf( "   latitude = %s\n", $geo->{latitude} || "<unknown>" );
        printf( "  longitude = %s\n", $geo->{longitude} || "<unknown>" );
    }

    $geo;
}


sub opencage_decode_timezone {
    my ($result_item) = @_;
    my $tz = undef;

    if (defined $result_item) {
        my $tz_info = $result_item->{annotations}->{timezone};
        if (defined $tz_info) {
            $tz->{timeZoneName} = $tz_info->{name};
            $tz->{timeZoneOffset} = $tz_info->{offset_sec} / 3600.0;
        }
    }

    if ($opt_d) {
        print  "opencage_decode_timezone: returning:\n";
        printf( "  timezone name = %s\n", $tz->{timeZoneName} || "<unknown>" );
        printf( "  tz offset hrs = %s\n", $tz->{timeZoneOffset} || "<unknown>" );
    }

    $tz;
}

sub format_address {
    my ($account) = @_;

    my $result = "";

    if (defined $account->{country} && $account->{country} eq 'CA') {
        if (defined $account->{address1}) {
            $result = sprintf( "%s, ", $account->{address1} );
        }
        if (defined $account->{city}) {
            $result .= sprintf( "%s, ", $account->{city} );
        }
        if (defined $account->{postalcode}) {
            $result .= sprintf( "%s ", $account->{postalcode} );
        }
        if (defined $account->{state}) {
            $result .= sprintf( "%s", $account->{state} );
        }
#        if (defined $account->{country}) {
#            $result .= sprintf( "%s", $account->{country} );
#        }
    }
    else {
        if (defined $account->{address1}) {
            $result = sprintf( "%s, ", $account->{address1} );
        }
        if (defined $account->{city}) {
            $result .= sprintf( "%s, ", $account->{city} );
        }
        if (defined $account->{state}) {
            $result .= sprintf( "%s ", $account->{state} );
        }
        if (defined $account->{postalcode}) {
            $result .= sprintf( "%s", $account->{postalcode} );
        }
#        if (defined $account->{country}) {
#            $result .= sprintf( "%s", $account->{country} );
#        }
    }

    $result;
}

sub print_dealer {
    my ($account) = @_;

    my $address = sprintf( "%s%s%s, %s%s, %s%s %s%s, %s", $WHITE,
        $account->{address1} || "$DIM<street address>$NORM",  $WHITE,
        $account->{city} || "$DIM<city>$NORM", $WHITE,
        $account->{state} || "$DIM<state>$NORM", $WHITE,
        $account->{postalcode} || "$DIM<postal code>$NORM", $WHITE,
        $account->{country} || "$DIM<country>$NORM" );

    my $latlng = sprintf("%s(LatLng: %s%s, %s)", $YELLOW,
        $account->{latitude} || "$DIM<unknown>$NORM", $YELLOW,
        $account->{longitude} || "$DIM<unknown>$NORM");

    my $timezonename =  $account->{timeZoneId} || "";
    my $timezoneid = defined $account->{timeZone} ? sprintf("(%d)", $account->{timeZone}) : "";
    my $tz = (isNullOrEmpty($timezonename) && isNullOrEmpty($timezoneid)) ? "" : sprintf("%sTimeZone: %s%s", $LOYELLOW, $timezonename, $timezoneid);

    printf( "%s%s%s :: %s %s %s%s\n", $GREEN, $account->{dealer_id} || "$DIM<unknown dealer>$NORM", $GREEN, $address, $latlng, $tz, $NORM );
}

sub distance {
    my ($lat1, $lng1, $lat2, $lng2) = @_;
    my $R = 6371; # radius of the earth in km
    my $φ1 = deg2rad( $lat1 );
    my $φ2 = deg2rad( $lat2 );
    my $Δφ = deg2rad( $lat2 - $lat1 );
    my $Δλ = deg2rad( $lng2 - $lng1 );

    my $a = sin( $Δφ / 2.0 ) * sin( $Δφ / 2.0 ) + cos( $φ1 ) * cos( $φ2 ) * sin( $Δλ / 2.0 ) * sin( $Δλ / 2.0 );
    my $c = 2.0 * atan2( sqrt( $a ), sqrt( 1.0 - $a ) );

    return $R * $c;
}


sub sprintf_distance {
    my ($dist) = @_;

    if ($dist < 1.00) {
        return sprintf( "%.2f meters", $dist * 1000 );
    }

    return sprintf( "%.2f kilometers", $dist );
}

sub isNullOrEmpty {
    my ($test) = @_;

    return (!defined( $test ) || $test eq "");
}

sub hasGeoCoordinates {
    my ($test) = @_;
    return !(isNullOrEmpty( $test->{latitude} ) || isNullOrEmpty( $test->{longitude} ));
}

sub hasAddress {
    my ($test) = @_;

    if (isNullOrEmpty($test->{country}) && !isNullOrEmpty($test->{state})) {
        # empty country, thought state is known
        if (defined $valid_US_states{$test->{state}}) {
            $test->{country} = 'US';
        }
    }

    return !(isNullOrEmpty( $test->{address1} ) ||
        isNullOrEmpty( $test->{city} ) ||
        isNullOrEmpty( $test->{state} ) ||
        isNullOrEmpty( $test->{country} ));
}

sub set_sleep_target {
    my ($target) = @_;

    if (defined $target) {
        $sleep_target = $target;
    }
    else {
        $sleep_target = $opt_S || 5;
    }
}

sub usage {
    print "usage: $0 [-d] [-v] -D <db> [-u <update_SQL_file>] [-r <revert_SQL_file>]\n";
    print "  -h :: print usage information.\n";
    print "  -d :: enable debugging output.\n";
    print "  -v :: enable verbose output.\n";
    print "  -n [ DEV | QA | QA_SLAVE | LIVE | LIVE_SLAVE ] :: select a nexus database to use.\n";
    print "  -u <update_SQL_file> :: specify the name of the SQL update file.\n";
    print "  -r <revert_SQL_file> :: specify the name of the SQL revert file.\n";
}
