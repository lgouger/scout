#!/usr/bin/env perl

my ($NORM, $BOLD, $DIM, $BLACK, $HIBLACK, $RED, $HIRED, $YELLOW, $HIYELLOW, $GREEN, $HIGREEN, $BLUE, $HIBLUE, $MAGENTA, $HIMAGENTA, $CYAN, $HICYAN, $WHITE, $HIWHITE);


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
    $YELLOW="\033[0;33m";
    $HIYELLOW="\033[1;33m";
    $BLUE="\033[0;34m";
    $HIBLUE="\033[1;34m";
    $MAGENTA="\033[0;35m";
    $HIMAGENTA="\033[1;35m";
    $CYAN="\033[0;36m";
    $HICYAN="\033[1;36m";
    $WHITE = "\033[0;37m";
    $HIWHITE = "\033[1;37m";
};


print $NORM, $BLACK, "TEST CONTENT (BLACK)", $BOLD, " the quick brown box jumped over the lazy dog. (HIBLACK)", $DIM, " Testing 1.2.3.4 (LOBLACK)\n";
print $NORM, $RED, "TEST CONTENT (RED)", $BOLD, " the quick brown box jumped over the lazy dog. (HIRED)", $DIM, " Testing 1.2.3.4 (LORED)\n";
print $NORM, $GREEN, "TEST CONTENT (GREEN)", $BOLD, " the quick brown box jumped over the lazy dog. (HIGREEN)", $DIM, " Testing 1.2.3.4 (LOGREEN)\n";
print $NORM, $YELLOW, "TEST CONTENT (YELLOW)", $BOLD, " the quick brown box jumped over the lazy dog. (HIYELLOW)", $DIM, " Testing 1.2.3.4 (LOYELLOW)\n";
print $NORM, $BLUE, "TEST CONTENT (BLUE)", $BOLD, " the quick brown box jumped over the lazy dog. (HIBLUE)", $DIM, " Testing 1.2.3.4 (LOBLUE)\n";
print $NORM, $MAGENTA, "TEST CONTENT (MAGENTA)", $BOLD, " the quick brown box jumped over the lazy dog. (HIMAGENTA)", $DIM, " Testing 1.2.3.4 (LOMAGENTA)\n";
print $NORM, $CYAN, "TEST CONTENT (CYAN)", $BOLD, " the quick brown box jumped over the lazy dog. (HICYAN)", $DIM, " Testing 1.2.3.4 (LOCYAN)\n";
print $NORM, $WHITE, "TEST CONTENT (WHITE)", $BOLD, " the quick brown box jumped over the lazy dog. (HIWHITE)", $DIM, " Testing 1.2.3.4 (LOWHITE)\n";
print $NORM;


