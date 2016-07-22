#!/usr/bin/env jjs -scripting

Number.prototype.toRad = function() {
    return this * Math.PI / 180;
}

function computeDistance(lat1, lon1, lat2, lon2) {
 
    var R = 6371; // radius of the earth in km
    var φ0 = 42.125;
    var φ1 = lat1.toRad();
    var φ2 = lat2.toRad();
    var Δφ = (lat2-lat1).toRad();
    var Δλ = (lon2-lon1).toRad();

    var a = Math.sin(Δφ/2) * Math.sin(Δφ/2) + Math.cos(φ1) * Math.cos(φ2) * Math.sin(Δλ/2) * Math.sin(Δλ/2);
    var c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));

    var d = R * c;

    return d;
}

// San Francisco:
var sf_latitude = 37.76834106;
var sf_longitude = -122.41825867;

var mb_latitude = 33.897;
var mb_longitude = -118.418;

var db_latitude = 40.2760879; 
var db_longitude = -74.7051859;

var geo_latitude = 40.2768958;
var geo_longitude = -74.705111;

var calgary_latitude = 51 + (3 / 60);   // 51° 03' N
var calgary_longitude = -1 * (114 + (4 / 60));   // 114° 04' W

var san_jose_latitude = 37 + (20 / 60);  // 37° 20' N
var san_jose_longitude = -1 * (121 + (53 / 60)); // 121° 53' W

print("Location 1 (from the Manhattan Beach)");
print("Latitude: " + mb_latitude + ", Longitude: " + mb_longitude + ".");
print("Location 2 (from the San Francisco)");
print("Latitude: " + sf_latitude + ", Longitude: " + sf_longitude + ".");
print("Distance between 1 and 2: " + computeDistance(mb_latitude, mb_longitude, sf_latitude, sf_longitude) + " kilometers.");

print("Location 1 (from the Calgary 51° 03' N, 114° 04' W)");
print("Latitude: " + calgary_latitude + ", Longitude: " + calgary_longitude + ".");
print("Location 2 (from the San Jose 37° 20' N, 121° 53' W)");
print("Latitude: " + san_jose_latitude + ", Longitude: " + san_jose_longitude + ".");
print("Distance between 1 and 2: " + computeDistance(calgary_latitude, calgary_longitude, san_jose_latitude, san_jose_longitude) + " kilometers.");

