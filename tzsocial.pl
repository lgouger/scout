#!/usr/bin/env perl


# support a list of accounts to include or a way to check all accounts

# support a FORCE argument (-f) to update timeZoneId whether it's currently set or not

# build a list of accounts with latitude, longitude and timeZone properties, but no timeZoneId property (so the script can be repeatedly run)


# Loop through the accounts 

# Use the Wunderground geolocation api to get a timeZoneId based on latitude and longitude

# compare new offset to stored offset, if different, report a warning.

# Save the TimeZoneId, generate SQL to insert a new timeZoneId property


use 5.012;
use utf8;

use warnings;
use strict;

use Getopt::Std;
use Term::ReadKey;
use DateTime;
use DateTime::TimeZone;
use FileHandle;
use File::Basename;
use Term::ReadKey;
use Math::Trig;
use List::Util qw(first all);
use List::MoreUtils qw(uniq);
use DBI;
use LPG::prompt qw(prompt_init prompt_for_value prompt_menu);
use LPG::SQLO;
use LPG::SQLO_prompt qw(sql_file_check);
use LPG::SQL_SOCIAL;
use LPG::SQL_NEXUS;

use LWP 5.64; # Loads all important LWP classes, and makes
#  sure your version is reasonably recent.
use URI::Escape;

use JSON;

our ($opt_h, $opt_d, $opt_v, $opt_n, $opt_D, $opt_u, $opt_r, $opt_R, $opt_S, $opt_A, $opt_f);

# -h                   :: print usage information.
# -d                   :: enable debugging output.
# -v                   :: enable verbose output.
# -A                   :: Pull ALL dealers
# -f                   :: force a lookup even if Geo data present
# -n [ DEV | QA | QA_SLAVE | LIVE | LIVE_SLAVE ] :: select a nexus database to use.
# -u <update_SQL_file> :: specify the name of the SQL update file.
# -r <revert_SQL_file> :: specify the name of the SQL revert file.
# -R <#>               :: randomly select # dealers
# -S <#>               :: sleep for # seconds between accounts

getopts( 'hdvAfnD:u:r:R:S:' );

$| = 1;

my ($NORM, $BOLD, $DIM, $BLACK, $RED, $YELLOW, $GREEN, $HIGREEN, $BLUE, $MAGENTA, $CYAN, $WHITE);
my ($rfh, $ufh);
my $geo_service = "ziptastic";

my $DEFAULT_UPDATE_FILENAME_FORMAT = "timezoneid_update_%s.sql";
my $DEFAULT_REVERT_FILENAME_FORMAT = "timezoneid_revert_%s.sql";

if (index( $ENV{TERM}, "color" ) != -1) {
    $NORM = "\033[0m";
    $BOLD = "\033[1m";
    $DIM = "\033[2m";
    $BLACK = "\033[0;30m";
    $RED = "\033[0;31m";
    $GREEN = "\033[0;32m";
    $HIGREEN = "\033[1;32m";
    $YELLOW = "\033[0;33m";
    $BLUE = "\033[0;34m";
    $MAGENTA = "\033[0;35m";
    $CYAN = "\033[0;36m";
    $WHITE = "\033[0;37m";
}


if ($opt_h) {
    usage();
    exit;
}

if (!defined( $opt_D )) {
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
    $opt_u = sprintf( $DEFAULT_UPDATE_FILENAME_FORMAT, $opt_D );
    printf( "No update SQL file specified, using default '%s'\n", $opt_u ) if ($opt_d);
}
sql_file_check($opt_u);

########################## SQL REVERT FILE CHECK/SELECTION ##########################
if (!defined $opt_r) {
    $opt_r = sprintf( $DEFAULT_REVERT_FILENAME_FORMAT, $opt_D );
    printf( "No revert SQL file specified, using default '%s'\n", $opt_r ) if ($opt_d);
}
sql_file_check($opt_r);


########################## DATABASE CONNECTION INIT ##########################

# Create a connection to the Social DB
my $dbo = LPG::SQLO->new($LPG::SQL_SOCIAL::social_db_normal_site_names{$opt_D}, \%LPG::SQL_SOCIAL::social_db_definitions);

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

my $sth = $connection->column_info( undef, undef, "temp_active_dealers", undef );
my $temp_active_dealers_columns = $sth->fetchall_hashref( 'COLUMN_NAME' );


########################## GENERATE DATABASE QUERY ##########################
my $query = "";
if ($opt_A || $opt_R) {
    $query = "select * from temp_active_dealers;";
    if (scalar @ARGV > 0) {
        print STDERR $YELLOW, "WARNING: Extra arguments being ignored because ", $WHITE, ($opt_A ? "-A" : "-R"), $YELLOW
            ,
            " argument present. (", $WHITE, join( " ", @ARGV ), $YELLOW, ")\n", $NORM;
    }
}
elsif (scalar @ARGV > 0) {
    my @quoted_dealer_ids = map { $connection->quote( $_,
        $temp_active_dealers_columns->{dealer_id}->{DATA_TYPE} ); } @ARGV;
    $query = sprintf( "select * from temp_active_dealers where dealer_id in (%s);", join( ",", @quoted_dealer_ids ) );
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
    printf( "Randomly selected %d dealer%s (%s)\n", $num_rows, ($num_rows == 1 ? "" : "s"),
        join ", ", map { $_->{dealer_id}; } @$rows );
}

if (!$num_rows) {
    print STDERR "No rows found.\n";
    exit;
}

printf( "%d total rows found.\n", $num_rows ) if ($opt_d);


##################### transform to uid key'd map #####################
my %accounts = ();
foreach my $row (@$rows) {
    $accounts{$row->{dealer_id}} = $row;
}


################# CATEGORIZE the ACCOUNTS #####################
my @no_geo_location = ();
my @no_location_at_all = ();
my @no_address = ();
foreach my $account_id (sort keys %accounts) {
    my $account = $accounts{$account_id};

    if (isNullOrEmpty( $account->{latitude} ) || isNullOrEmpty( $account->{longitude} )) {
        # no geographic coordinates
        if (isNullOrEmpty( $account->{address1} ) ||
            isNullOrEmpty( $account->{city} ) ||
            isNullOrEmpty( $account->{state} ) ||
            isNullOrEmpty( $account->{country} ))
        {
            push @no_location_at_all, $account_id;
        }
        else {
            push @no_geo_location, $account_id;
        }
    }
    else {
        # latitude and longitude present
        # do they have an address on file too?
        if (isNullOrEmpty( $account->{address1} ) ||
            isNullOrEmpty( $account->{city} ) ||
            isNullOrEmpty( $account->{state} ) ||
            isNullOrEmpty( $account->{country} ))
        {
            # no address,
            push @no_address, $account_id;
        }
    }
}


###############  NOT ENOUGH ADDRESS and NO LAT/LON, so Remove from the accounts list  ###############
if (scalar @no_location_at_all > 0) {
    printf( "%sThe following %d dealers have no known address nor geographic coordinates.\n%s", $RED,
        scalar @no_location_at_all, $NORM );

    foreach my $account_id (@no_location_at_all) {
        my $account = $accounts{$account_id};

        print "    ", $YELLOW, $account->{dealer_id}, ": ", $WHITE;
        printf( "%s, %s, %s %s, %s%s\n",
            $account->{address1} || "<street address>",
            $account->{city} || "<city>",
            $account->{state} || "<state>",
            $account->{postalcode} || "<postal code>",
            $account->{country} || "<country>",
            $NORM);

        # remove it from the accounts collection, there's nothing we can do.
        delete $accounts{$account_id};
    }
}



##################### Create Browser Like Thing #####################
my $browser = LWP::UserAgent->new;
$browser->ssl_opts( verify_hostname => 0 );


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


############### NO LAT/LON (HOWEVER ADDRESS IS PRESENT), so lookup lat/lng ###############
if (scalar @no_geo_location > 0) {
    printf( "%s%d dealers with no latitude/longitude stored.\n%s", $YELLOW, scalar @no_geo_location, $NORM );

    my $left = scalar @no_geo_location;
    foreach my $account_id (@no_geo_location) {
        my $account = $accounts{$account_id};

        print "    ", $YELLOW, $account->{dealer_id}, ": ", $WHITE;
        printf( "%s, %s, %s %s, %s\n",
            $account->{address1} || "<street address>",
            $account->{city} || "<city>",
            $account->{state} || "<state>",
            $account->{postalcode} || "<postal code>",
            $account->{country} || "<country>" );

        if (!$opt_n) {
            ## look up the latitude and longitude using the address
            ## -- $geo is a hash with "status", "latitude" and "longitude" fields.
            my $geo = get_geoloc( $browser, $account );

            if (defined( $geo )) {
                my $update_location = "update temp_active_dealers set latitude=%s, longitude=%s where dealer_id=%s;";
                my $revert_command = sprintf( $update_location,
                    $connection->quote( $accounts{$account_id}->{latitude}, $info->{timeZone}->{DATA_TYPE} ),
                    $connection->quote( $accounts{$account_id}->{longitude}, $info->{timeZone}->{DATA_TYPE} ),
                    $connection->quote( $account->{dealer_id}, $info->{dealer_id}->{DATA_TYPE} ) );

                my $update_command = sprintf( $update_location,
                    $connection->quote( $geo->{latitude}, $info->{timeZone}->{DATA_TYPE} ),
                    $connection->quote( $geo->{longitude}, $info->{timeZone}->{DATA_TYPE} ),
                    $connection->quote( $account->{dealer_id}, $info->{dealer_id}->{DATA_TYPE} ) );

                if (defined $rfh) {
                    $rfh->printf( "%s\n", $revert_command );
                }
                if (defined $ufh) {
                    $ufh->printf( "%s\n", $update_command );
                }

                $accounts{$account_id}->{latitude} = $geo->{latitude};
                $accounts{$account_id}->{longitude} = $geo->{longitude};
            }

            $left--;

            if ($left > 0) {
                my $sleep_time = int( rand( $sleep_target ) );
                printf( "Sleeping for %d seconds.\n", $sleep_time ) if $opt_d;
                sleep $sleep_time;
            }
        }
    }
}


############### NO ADDRESS (HOWEVER LAT/LON IS PRESENT), so lookup address ###############
if (scalar @no_address > 0) {
    printf( "%s%d accounts with no address information.\n%s", $YELLOW, scalar @no_address, $NORM );

    my $left = scalar @no_address;
    foreach my $account_id (@no_address) {
        my $account = $accounts{$account_id};

        print $GREEN, $account->{dealer_id}, " :: ", $YELLOW, "no address though lat/lon location known.\n", $NORM;

        # disable until we save the updated address
        if (!$opt_n) {
            ## look up the address using the latitude and longitude
            my $revgeo = get_reverse_geoloc( $browser, $account->{latitude}, $account->{longitude} );

            if (defined( $revgeo )) {

                if ($opt_d) {
                    print "Found via Google Reverse Geocoding API, ";
                    printf( "%s%s :: %s(Lat: %s, Lon: %s)%s\n", $GREEN, $account->{dealer_id}, $YELLOW,
                        $account->{latitude}, $account->{longitude}, $NORM );
                }

                my $address = undef;
                if (defined( $revgeo->{results} ) && scalar @{$revgeo->{results}} > 0) {
                    $address = google_decode_address_components( ${$revgeo->{results}}[0] );

                    if (defined $address) {
                        # clear any old address
                        $accounts{$account_id}->{address1} = $address->{address1};
                        $accounts{$account_id}->{city}     = $address->{city};
                        $accounts{$account_id}->{state}    = $address->{state};
                        $accounts{$account_id}->{country}  = $address->{country};
                        $accounts{$account_id}->{postalcode} = $address->{postalcode};
                    }
                }
                else {
                    print $RED, "No results found in reverse geolocation response, only (", join( ", ", keys %$revgeo ), ")\n", $NORM;
                }
            }

            $left--;

            if ($left > 0) {
                my $sleep_time = int( rand( $sleep_target ) );
                printf( "Sleeping for %d seconds.\n", $sleep_time ) if $opt_d;
                sleep $sleep_time;
            }
        }
    }
}


# At this point should have address and geo coordinates
my $left = scalar @$rows;
foreach my $account_id (sort keys %accounts) {
    print $HIGREEN, "\n===== ", $account_id, " =====\n", $NORM;
    my $account = $accounts{$account_id};

    # skip rows that already have a timeZoneId defined. 
    if (!$opt_f && defined( $account->{timeZoneId} && $account->{timeZoneId} ne "" )) {
        print $GREEN, "Already have timeZoneId and forced update not requested.\n", $NORM;
        next;
    }

    if (!defined( $account->{latitude} ) || $account->{latitude} eq "" || !defined( $account->{longitude} ) || $account->{longitude} eq "") {
        print $RED, "No timeZoneId lookup performed because incomplete geo coordinates found.\n", $NORM;
        next;
    }

    my $address = sprintf( "%s, %s, %s %s, %s",
        $account->{address1} || "<street address>",
        $account->{city} || "<city>",
        $account->{state} || "<state>",
        $account->{postalcode} || "<postal code>",
        $account->{country} || "<country>" );

    printf( "%s%s%s :: %s %s(Lat: %s, Lon: %s)%s\n", $GREEN, $account->{dealer_id} || "<unknown dealer>",
        $WHITE, $address, $YELLOW,
        $account->{latitude} || "<unknown>",
        $account->{longitude} || "<unknown>",
        $NORM );

    if (!$opt_n) {
        my $tz = get_timezone( $browser, $account->{latitude}, $account->{longitude} );

        if (defined( $tz )) {
            # warn if timezone offset differs from the one stored in the database
            print $account->{dealer_id}, ": ", $GREEN, $tz->{timeZoneName}, " (", $tz->{timeZoneId}, ") an offset of ",
                $tz->{hours_offset}, " hours\n", $NORM;

            if (!defined( $account->{timeZone} ) || ($account->{timeZone} != $tz->{hours_offset})) {
                print $account->{dealer_id}, ": ", $RED, "Timezone offset in database, ",
                    (defined( $account->{timeZone} ) ? $account->{timeZone} : "<undefined>"),
                    ", does not match ", $tz->{hours_offset}, " received via the API.", $NORM, "\n";

                # timeZoneId property :: create the commands to revert and update
                my $update_timeZone = "update temp_active_dealers set timeZone=%s where dealer_id=%s;";
                my $old_timezone = defined $account->{timeZone} ? ($account->{timeZone} eq "NULL" ? "NULL" : sprintf("%d", $account->{timeZone})) : "NULL";
                my $revert_command = sprintf( $update_timeZone,
                    $connection->quote( $old_timezone, $info->{timeZone}->{DATA_TYPE} ),
                    $connection->quote( $account->{dealer_id}, $info->{dealer_id}->{DATA_TYPE} ) );

                my $update_command = sprintf( $update_timeZone,
                    $connection->quote( sprintf("%d", $tz->{hours_offset}), $info->{timeZone}->{DATA_TYPE} ),

                    $connection->quote( $account->{dealer_id}, $info->{dealer_id}->{DATA_TYPE} ) );

                if (defined $rfh) {
                    $rfh->printf( "%s\n", $revert_command );
                }
                if (defined $ufh) {
                    $ufh->printf( "%s\n", $update_command );
                }
            }

            # timeZoneId property :: create the commands to revert and update
            my $update_timeZoneId = "update temp_active_dealers set timeZoneId=%s where dealer_id=%s;";
            my $revert_command = sprintf( $update_timeZoneId,
                $connection->quote( $account->{timeZoneId}, $info->{timeZoneId}->{DATA_TYPE} ),
                $connection->quote( $account->{dealer_id}, $info->{dealer_id}->{DATA_TYPE} ) );

            my $update_command = sprintf( $update_timeZoneId,
                $connection->quote( $tz->{timeZoneId}, $info->{timeZoneId}->{DATA_TYPE} ),
                $connection->quote( $account->{dealer_id}, $info->{dealer_id}->{DATA_TYPE} ) );

            if (defined $rfh) {
                $rfh->printf( "%s\n", $revert_command );
            }
            if (defined $ufh) {
                $ufh->printf( "%s\n", $update_command );
            }

            # update the accounts map entry in case I want to use it later
            $accounts{$account_id}->{timeZoneId} = $tz->{timeZoneId};
        }

        $left--;

        if ($left > 0) {
            my $sleep_time = int( rand( $sleep_target ) );
            printf( "Sleeping for %d seconds.\n", $sleep_time ) if $opt_d;
            sleep $sleep_time;
        }
    }
}

if (defined $rfh) {
    $rfh->close;
}
if (defined $ufh) {
    $ufh->close;
}

exit;


# function: get_geoloc
# @params: account hash
# @returns hash with the following fields:
# - status
# - latitde
# - longitude
sub get_geoloc {
    my ($browser, $data_item) = @_;
    my $result = undef;

    if ($geo_service eq "google") {
        $result = get_google_geoloc_info($browser, $data_item);
    }
    elsif ($geo_service eq "ziptastic") {
        $result = get_ziptastic_geoloc_info($browser, $data_item);
    }

    return $result;
}

sub get_ziptastic_geoloc_info {
    my ($browser, $account) = @_;

    print "Using Ziptastic to lookup the lat/lng for the location, $account->{postalcode}\n" if $opt_d;

    my $ziptastic_geoloc_api_url = sprintf( "https://zip.getziptastic.com/v3/%s/%s", substr($account->{country},0,2), substr($account->{postalcode},0,5) );

    my $response = $browser->get( $ziptastic_geoloc_api_url, 'x-key' => '5893afab93d2dd2f6b111d70b06e92c4702b7044' );

    if (!$response->is_success) {
        print $RED, "Can't get $ziptastic_geoloc_api_url -- ", $NORM, $response->status_line, "\n";
    }
    else {
        print $GREEN, "GET $ziptastic_geoloc_api_url -- ", $NORM, $response->status_line, "\n";

        if ($opt_d) {
            print $GREEN, "content_type: ", $WHITE, $response->content_type, $NORM, "\n";
            print $RED, "response:\n", $NORM, $response->content, "\n";
        }

        my $result = decode_json $response->content;

        return {
            "latitude"  => ${$result}[0]->{latitude},
            "longitude" => ${$result}[0]->{longitude}
        };
    }

    return undef;
}


sub get_google_geoloc_info {
    my ($browser, $account) = @_;

    my $address = sprintf( "%s, %s, %s  %s", $account->{address1}, $account->{city}, $account->{state}, $account->{postalcode} );

    printf( "%s%s%s :: %s %s(Lat: %s, Lon: %s)%s\n", $GREEN, $account->{dealer_id}, $WHITE, $address, $YELLOW,
        $account->{latitude} || "<unknown>",
        $account->{longitude} || "<unknown>", $NORM );

    my $google_geocode_api_url = sprintf( "https://maps.googleapis.com/maps/api/geocode/json?address=%s&key=%s",
        uri_escape( $address ), "AIzaSyAT-FsZkd7HuxZDHGk5HUofnzN71ZrFxHA" );

    my $response = $browser->get( $google_geocode_api_url );
    if (!$response->is_success) {
        print $RED, "Can't get $google_geocode_api_url -- ", $NORM, $response->status_line, "\n";
    }
    else {
        print $GREEN, "GET $google_geocode_api_url -- ", $NORM, $response->status_line, "\n";

        print $GREEN, "content_type: ", $WHITE, $response->content_type, $NORM, "\n";

        # Otherwise, process the content somehow:

        print $RED, "response:\n", $NORM, $response->content if ($opt_d);

        my $resultref = decode_json $response->content;

        if ($resultref->{status} eq 'OK') {
            set_sleep_target(); # sleep for default amount (5 secs or -S #)
            my $result = ${$resultref->{results}}[0];

            if ($opt_d) {
                print "Found via Google Geocoding API, ";
                printf( "%s%s%s :: %s %s(Lat: %s, Lon: %s)%s\n", $GREEN, $account->{dealer_id}, $WHITE,
                    $result->{formatted_address}, $YELLOW,
                    $result->{geometry}->{location}->{lat}, $result->{geometry}->{location}->{lng}, $NORM );
            }

            return {
                "latitude"  => $result->{geometry}->{location}->{lat},
                "longitude" => $result->{geometry}->{location}->{lng},
            };

        }
        elsif ($resultref->{status} eq 'OVER_QUERY_LIMIT') {
            set_sleep_target( 3600 ); # sleep for an hour
        }

        print $RED, "geocode query returned: ", $resultref->{status}, ": ", $resultref->{errorMessage}, $NORM, "\n";
    }

    return undef;
}


# function: get_reverse_geoloc
# @params: latitude, longitude
# @returns hash with the following fields
#  - address1
#  - city
#  - state
#  - country
#  - postalcode
sub get_reverse_geoloc {
    my ($browser, $target_postalcode, $latitude, $longitude) = @_;
    my $result = undef;

    if ($geo_service eq "google") {
        $result = get_google_reverse_geoloc_info($browser, $latitude, $longitude);
    }
    elsif ($geo_service eq "ziptastic") {
        $result = get_ziptastic_reverse_geoloc_info($browser, $target_postalcode, $latitude, $longitude);
    }

    return $result;
}

sub get_ziptastic_reverse_geoloc_info {
    my ($browser, $target_postalcode, $latitude, $longitude) = @_;

    print "Using Ziptastic to lookup the address for the location, $latitude,$longitude\n" if $opt_d;

    my $ziptastic_reverse_geoloc_api_url = sprintf("https://zip.getziptastic.com/v3/reverse/%s/%s", $latitude, $longitude );

    my $response = $browser->get( $ziptastic_reverse_geoloc_api_url, 'x-key' => '5893afab93d2dd2f6b111d70b06e92c4702b7044' );

    if (!$response->is_success) {
        print $RED, "Can't get $ziptastic_reverse_geoloc_api_url -- ", $NORM, $response->status_line, "\n";
    }
    else {
        print $GREEN, "GET $ziptastic_reverse_geoloc_api_url -- ", $NORM, $response->status_line, "\n";

        if ($opt_d) {
            print $GREEN, "content_type: ", $WHITE, $response->content_type, $NORM, "\n";
            print $RED, "response:\n", $NORM, $response->content, "\n";
        }

        my $result = decode_json $response->content;

        # $result is an array ref
        my @zips = map { $_->{postal_code} } @{$result};
        my @zones = map { $_->{timezone} } @{$result};
        my @uniq_zones = uniq @zones;
        my $all_or_nothing = (scalar @uniq_zones == 1);
        printf("%d zip codes found (%s) %s %s\n", scalar @zips, join(", ", @zips),
                $all_or_nothing ? "all in" : "in timezones ",
                $all_or_nothing ? $zones[0] : join(", ", @uniq_zones));

        my $match = ${$result}[0];
        if (defined $target_postalcode) {
            $match = first { $_->{postal_code} eq $target_postalcode } @{$result} || ${$result}[0];
        }

        return {
            address1 => undef,
            city => $match->{city},
            state => $match->{state_short},
            country => $match->{country},
            postalcode => $match->{postal_code}
        };
    }

    return undef;
}

sub get_google_reverse_geoloc_info {
    my ($browser, $latitude, $longitude) = @_;

    print "Using Google to lookup the address for the location, $latitude,$longitude\n" if $opt_d;

    my $google_reverse_geocode_api_url = sprintf(
        "https://maps.googleapis.com/maps/api/geocode/json?latlng=%s,%s&key=%s",
        $latitude, $longitude, "AIzaSyAT-FsZkd7HuxZDHGk5HUofnzN71ZrFxHA" );

    my $response = $browser->get( $google_reverse_geocode_api_url );
    if (!$response->is_success) {
        print $RED, "Can't get $google_reverse_geocode_api_url-- ", $NORM, $response->status_line, "\n";
    }
    else {
        print $GREEN, "GET $google_reverse_geocode_api_url-- ", $NORM, $response->status_line, "\n";

        if ($opt_d) {
            print $GREEN, "content_type: ", $WHITE, $response->content_type, $NORM, "\n";
            print $RED, "response:\n", $NORM, $response->content;
        }

        my $revgeo = decode_json $response->content;

        if (defined( $revgeo )) {
            if ($revgeo->{status} eq 'OK') {
                set_sleep_target(); # return to default sleep

                if ($opt_d) {
                    printf( "%sFound via Google Reverse Geocoding API :: (%sLat: %s, Lon: %s%s)\n", $GREEN, $YELLOW,
                        $latitude, $longitude, $NORM );
                    foreach my $addr (@{$revgeo->{results}}) {
                        print $GREEN, '    "formatted_address" : ', $WHITE, $addr->{formatted_address}, $NORM, "\n";
                    }
                }

                my $address = undef;
                if (defined( $revgeo->{results} ) && scalar @{$revgeo->{results}} > 0) {
                    $address = google_decode_address_components( ${$revgeo->{results}}[0] );

                    if (defined $address) {
                        return {
                            address1 => $address->{address1},
                            city => $address->{city},
                            state => $address->{state},
                            country => $address->{country},
                            postalcode => $address->{postalcode}
                        };
                    }
                }
                else {
                    print $RED, "No results found in reverse geolocation response, only (", join( ", ", keys %$revgeo ), ")\n", $NORM;
                }
            }
            elsif ($revgeo->{status} eq 'OVER_QUERY_LIMIT') {
                set_sleep_target( 3600 ); # sleep for an hour
            }
        }

        print $RED, "reverse geolocation query returned: ", $revgeo->{status}, ": ", $revgeo->{errorMessage}, $NORM, "\n";
    }

    return undef;
}

sub google_decode_address_components {
    my ($detailed_address) = @_;
    my $result = {
        "address1" => "",
        "city"     => "",
        "state"    => "",
        "country"  => "",
        "postalcode" => ""
    };

    if (defined($detailed_address->{address_components}) && scalar @{$detailed_address->{address_components}} > 0) {

        foreach my $address_component (@{$detailed_address->{address_components}}) {
            if ($opt_d) {
                print $MAGENTA, "google_decode_address_components: { component: [", join( ", ", @{$address_component->{types}} ), "], long: \"", $address_component->{long_name},
                    "\", short: \"", $address_component->{short_name}, "\"}\n", $NORM;
            }

            if (first { $_ eq "street_number" } @{$address_component->{types}}) {
                #  street number
                if (isNullOrEmpty( $result->{address1} )) {
                    $result->{address1} = $address_component->{short_name};
                }
                else {
                    $result->{address1} = $address_component->{short_name}." ".$result->{address1};
                }
                printf( "%s :: using \"%s\" to create \"%s\"\n", "street_number",
                    $address_component->{short_name}, $result->{address1} ) if $opt_d;
            }
            elsif (first { $_ eq "route" } @{$address_component->{types}}) {
                # street name
                if (isNullOrEmpty( $result->{address1} )) {
                    $result->{address1} = $address_component->{long_name};
                }
                else {
                    $result->{address1} = $result->{address1}." ".$address_component->{long_name};
                }
                printf( "%s :: using \"%s\" to create \"%s\"\n", "route",
                    $address_component->{long_name}, $result->{address1} ) if $opt_d;
            }
            elsif (first { $_ eq "locality" } @{$address_component->{types}}) {
                # city
                $result->{city} = $address_component->{long_name};
                printf( "%s :: using \"%s\" to create \"%s\"\n", "locality",
                    $address_component->{long_name}, $result->{city} ) if $opt_d;
            }
            elsif (first { $_ eq "sublocality" } @{$address_component->{types}}) {
                # alternative way to get the city, if it isn't already set
                if (isNullOrEmpty( $result->{city} )) {
                    $result->{city} = $address_component->{long_name};
                    printf( "%s :: using \"%s\" to create \"%s\"\n", "sublocality",
                        $address_component->{long_name}, $result->{city} ) if $opt_d;
                }
            }
            elsif (first { $_ eq "administrative_area_level_1" } @{$address_component->{types}}) {
                # US: state
                $result->{state} = $address_component->{short_name};
                printf( "%s :: using \"%s\" to create \"%s\"\n", "administrative_area_level_1",
                    $address_component->{short_name}, $result->{state} ) if $opt_d;
            }
            elsif (first { $_ eq "country" } @{$address_component->{types}}) {
                # country
                $result->{country} = $address_component->{short_name};
                printf( "%s :: using \"%s\" to create \"%s\"\n", "country",
                    $address_component->{short_name}, $result->{country} ) if $opt_d;
            }
            elsif (first { $_ eq "postal_code" } @{$address_component->{types}}) {
                # postal code
                $result->{postalcode} = $address_component->{short_name};
                printf( "%s :: using \"%s\" to create \"%s\"\n", "postal_code",
                    $address_component->{short_name}, $result->{postalcode} ) if $opt_d;
            }
            else {
                print $CYAN, "Ignoring address components: ", join( ", ", @{$address_component->{types}} ), "\n", $NORM if $opt_d;
            }
        }

        return $result;
    }
    else {
        print $RED, "No address components found in detailed address, only (", join( ", ", keys %$detailed_address ), ")\n", $NORM;
    }
}


# function: get_timezone
# @params: latitude, longitude
# @returns hash with the following fields
#  - housr_offset
#  - timeZoneId
#  - timeZoneName
sub get_timezone {
    my ($browser, $latitude, $longitude) = @_;
    my $result = undef;

    if ($geo_service eq "google") {
        $result = get_google_timezone_info($browser, $latitude, $longitude);
    }
    elsif ($geo_service eq "ziptastic") {
        $result = get_ziptastic_timezone_info($browser, $latitude, $longitude);
    }

    return $result;
}

sub get_google_timezone_info {
    my ($browser, $latitude, $longitude) = @_;

    print "Using Google to lookup the timezone for the location, $latitude,$longitude\n" if $opt_d;

    my $google_timezone_api_url = sprintf(
        "https://maps.googleapis.com/maps/api/timezone/json?location=%s,%s&timestamp=%s&key=%s",
        $latitude, $longitude, time(), "AIzaSyAT-FsZkd7HuxZDHGk5HUofnzN71ZrFxHA" );

    my $response = $browser->get( $google_timezone_api_url );
    if (!$response->is_success) {
        print $RED, "Can't get $google_timezone_api_url -- ", $NORM, $response->status_line, "\n";
    }
    else {
        print $GREEN, "GET $google_timezone_api_url -- ", $NORM, $response->status_line, "\n";

        if ($opt_d) {
            print $GREEN, "content_type: ", $WHITE, $response->content_type, $NORM, "\n";
            print $RED, "response:\n", $NORM, $response->content;
        }

        my $result = decode_json $response->content;

        if ($result->{status} eq 'OK') {
            set_sleep_target();

            return {
                "hours_offset" => ($result->{rawOffset} + $result->{dstOffset}) / 3600.0,
                "timeZoneId"   => $result->{timeZoneId},
                "timeZoneName" => $result->{timeZoneName}
            };
        }
        elsif ($result->{status} eq 'OVER_QUERY_LIMIT') {
            set_sleep_target( 3600 ); # sleep for an hour
        }

        print $RED, "timezone query returned: ", $result->{status}, ": ", $result->{errorMessage}, $NORM, "\n";
    }

    return undef;
}

sub get_ziptastic_timezone_info {
    my ($browser, $latitude, $longitude) = @_;

    print "Using Ziptastic to lookup the timezone for the location, $latitude,$longitude\n" if $opt_d;

    my $ziptastic_timezone_api_url = sprintf("https://zip.getziptastic.com/v3/reverse/%s/%s", $latitude, $longitude );

    my $response = $browser->get( $ziptastic_timezone_api_url, 'x-key' => '5893afab93d2dd2f6b111d70b06e92c4702b7044' );

    if (!$response->is_success) {
        print $RED, "Can't get $ziptastic_timezone_api_url -- ", $NORM, $response->status_line, "\n";
    }
    else {
        print $GREEN, "GET $ziptastic_timezone_api_url -- ", $NORM, $response->status_line, "\n";

        if ($opt_d) {
            print $GREEN, "content_type: ", $WHITE, $response->content_type, $NORM, "\n";
            print $RED, "response:\n", $NORM, $response->content, "\n";
        }

        my $result = decode_json $response->content;

        # $result is an array ref
        my @zips = map { $_->{postal_code} } @{$result};
        my @zones = map { $_->{timezone} } @{$result};
        my @uniq_zones = uniq @zones;
        my $all_or_nothing = (scalar @uniq_zones == 1);
        printf("%d zip codes found (%s) %s %s\n", scalar @zips, join(", ", @zips),
                $all_or_nothing ? "all in" : "in timezones ",
                $all_or_nothing ? $zones[0] : join(", ", @uniq_zones));

        my $now = DateTime->now();
        my $dt = DateTime->new(year => $now->year, time_zone => "UTC" );

        print "Test day: ", $dt->ymd(), " @ ", $dt->hms(), "\n";

        my $tz = DateTime::TimeZone->new( name => $zones[0] );

        my $offset = $tz->offset_for_datetime($dt);

        return {
            "hours_offset" => $offset / 3600.0,
            "timeZoneId"   => $zones[0],
            "timeZoneName" => ""
        };
    }

    return undef;
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

sub set_sleep_target {
    my ($target) = @_;

    if (defined $target) {
        $sleep_target = $target;
    }
    else {
        $sleep_target = $opt_S || 5;
    }
}

sub sub_field_ordering {
    my ($keya, $keyb, $href, $field) = @_;
    my $sub_href_a = $href->{$keya};
    my $sub_href_b = $href->{$keyb};

    $sub_href_a->{$field} <=> $sub_href_b->{$field};
}

sub isNullOrEmpty {
    my ($test) = @_;

    return (!defined( $test ) || $test eq "");
}

sub usage {
    print "usage: $0 [-d] -n <db> [-u <update_SQL_file>] [-r <revert_SQL_file>]\n";
    print "  -h :: print usage information.\n";
    print "  -d :: enable debugging output.\n";
    print "  -n [ DEV | QA | QA_SLAVE | LIVE | LIVE_SLAVE ] :: select a nexus database to use.\n";
    print "  -u <update_SQL_file> :: specify the name of the SQL update file.\n";
    print "  -r <revert_SQL_file> :: specify the name of the SQL revert file.\n";
}



