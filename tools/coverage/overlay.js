var overlays = {};
var markers = {};
var map = null;

function addButtonFor(station, val) {
        var template = document.getElementById("station-template");
        var clone = template.cloneNode(true);
        clone.classList.remove("hidden");
        var button = clone.querySelector("button");
        button.innerHTML = station;
        button.addEventListener("click", selectOverlay.bind(undefined, val));
        document.getElementById("station-table-body").appendChild(clone);
}

function addBlankRow() {
        var template = document.getElementById("station-template");
        var clone = template.cloneNode(true);
        clone.classList.remove("hidden");
        var button = clone.querySelector("button");
        button.parentNode.removeChild(button);
        document.getElementById("station-table-body").appendChild(clone);
}

function initialize() {
        document.getElementById("date_start").innerHTML = first_position;
        document.getElementById("date_end").innerHTML = last_position;
        document.getElementById("num_pos").innerHTML = num_positions;

        map = new google.maps.Map(document.getElementById('map-canvas'));

        var absbounds = null;

        addButtonFor("All coverage", "all");
        addBlankRow();
        addButtonFor("4+ station overlap", "4plus");
        addButtonFor("5+ station overlap", "5plus");
        addButtonFor("6+ station overlap", "6plus");
        addButtonFor("Below 18000ft", "below18000");
        addButtonFor("Below 10000ft", "below10000");
        addButtonFor("Min altitude seen", "byalt");
        addBlankRow();

        var names = Object.keys(coverage).sort();
        for (var i = 0; i < names.length; ++i) {
                var k = names[i];
                var s = coverage[k];
                var bounds = new google.maps.LatLngBounds(
                        new google.maps.LatLng(s.min_lat, s.min_lon),
                        new google.maps.LatLng(s.max_lat, s.max_lon));

                overlays[k] = new google.maps.GroundOverlay(
                        s.image,
                        bounds,
                        { opacity : 1.0 });

                if (absbounds === null) {
                        absbounds = new google.maps.LatLngBounds(bounds.getSouthWest(), bounds.getNorthEast());
                } else { 
                        absbounds.union(bounds);
                }

                if (s.lat !== null) {
                        // marker jitter is just to separate markers that would otherwise be overlapping
                        markers[k] = new google.maps.Marker({
                                map : map,
                                position : new google.maps.LatLng(s.lat + Math.random()*0.02-0.01, s.lon + Math.random()*0.02-0.01),
                                title : s.name
                        });

		        google.maps.event.addListener(markers[k], 'click', selectOverlay.bind(undefined, k))
                }

                if (s.is_station) {
                        addButtonFor(k, k);
                }
        }

        overlays['all'].setMap(map);
        map.fitBounds(absbounds);
}

var currentOverlay = "all";
function selectOverlay(stationname) {
        overlays[currentOverlay].setMap(null);

        if (currentOverlay === stationname) {
                stationname = "all";
        }
        
        overlays[stationname].setMap(map);
        currentOverlay = stationname;
}

google.maps.event.addDomListener(window, 'load', initialize);
