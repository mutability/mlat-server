function refresh() {
        var xhr = new XMLHttpRequest();
        xhr.onreadystatechange = function() {
                if (xhr.readyState == 4) {
                        var stateObj = JSON.parse(xhr.responseText);
                        rebuildTable(stateObj);
                }
        };

        var cachebust = new Date().getTime();
        xhr.open("GET", "sync.json?" + cachebust, true);
        xhr.send();
}

function rebuildTable(state) {
        var table = document.getElementById("syncstatstable");
        while (table.firstChild) {
                table.removeChild(table.firstChild);
        }

        var receivers = Object.keys(state);
        receivers.sort();
        var header_row = document.createElement('tr');

        var header_td = document.createElement('td');
        header_td.innerHTML = "&nbsp;";
        header_row.appendChild(header_td);        

        for (var i = 0; i < receivers.length; ++i) {
                header_td = document.createElement('td');
                header_td.colSpan = "2";
                header_td.innerHTML = receivers[i];
                header_row.appendChild(header_td);
        }

        header_td = document.createElement('td');
        header_td.innerHTML = "&nbsp;";
        header_row.appendChild(header_td);

        table.appendChild(header_row);
        
        for (var i = 0; i < receivers.length; ++i) {
                var data_row_1 = document.createElement('tr');
                var data_row_2 = document.createElement('tr');

                var header_col = document.createElement('td');
                header_col.innerHTML = receivers[i];
                header_col.rowSpan = "2";
                data_row_1.appendChild(header_col);
                
                var receiver_state = state[receivers[i]].peers;
                for (var j = 0; j < receivers.length; ++j) {
                        var data_cell;
                        if (i == j) {
                                data_cell = document.createElement('td');
                                data_cell.colSpan = "2";
                                data_cell.rowSpan = "2";
                                data_cell.className = "sync_count sync_omit";
                                data_cell.innerHTML = receivers[i];
                                data_row_1.appendChild(data_cell);
                        } else if (receivers[j] in receiver_state) {
                                var syncstate = receiver_state[receivers[j]];

                                data_cell = document.createElement('td');
                                data_cell.innerHTML = syncstate[0];
                                if (syncstate[0] >= 10) {
                                        data_cell.className = "sync_count sync_good";
                                } else {
                                        data_cell.className = "sync_count sync_ok";
                                }
                                data_row_1.appendChild(data_cell);

                                data_cell = document.createElement('td');
                                data_cell.innerHTML = syncstate[1].toFixed(1);
                                if (syncstate[1] <= 2.0) {
                                        data_cell.className = "sync_err sync_good";
                                } else if (syncstate[1] <= 4.0) {
                                        data_cell.className = "sync_err sync_ok";
                                } else {
                                        data_cell.className = "sync_err sync_bad";
                                }
                                data_row_1.appendChild(data_cell);

                                data_cell = document.createElement('td')
                                data_cell.innerHTML = syncstate[2].toFixed(2);
                                if (Math.abs(syncstate[2]) <= 50.0) {
                                        data_cell.className = "sync_ppm sync_good";
                                } else if (Math.abs(syncstate[2]) <= 180.0) {
                                        data_cell.className = "sync_ppm sync_ok";
                                } else {
                                        data_cell.className = "sync_ppm sync_bad";
                                }
                                data_cell.colSpan = "2";
                                data_row_2.appendChild(data_cell);

                        } else {
                                data_cell = document.createElement('td');
                                data_cell.innerHTML = "&nbsp;";
                                data_cell.className = "sync_count sync_nodata";
                                data_row_1.appendChild(data_cell);

                                data_cell = document.createElement('td');
                                data_cell.innerHTML = "&nbsp;";
                                data_cell.className = "sync_err sync_nodata";
                                data_row_1.appendChild(data_cell);

                                data_cell = document.createElement('td')
                                data_cell.innerHTML = "&nbsp;";
                                data_cell.className = "sync_ppm sync_nodata";
                                data_cell.colSpan = "2";
                                data_row_2.appendChild(data_cell);
                        }
                }

                header_col = document.createElement('td');
                header_col.innerHTML = receivers[i];
                header_col.rowSpan = "2";
                data_row_1.appendChild(header_col);                
                
                table.appendChild(data_row_1);
                table.appendChild(data_row_2);
        }

        var footer_row = document.createElement('tr');

        var footer_td = document.createElement('td');
        footer_td.innerHTML = "&nbsp;";
        footer_row.appendChild(footer_td);        

        for (var i = 0; i < receivers.length; ++i) {
                footer_td = document.createElement('td');
                footer_td.colSpan = "2";
                footer_td.innerHTML = receivers[i];
                footer_row.appendChild(footer_td);
        }

        footer_td = document.createElement('td');
        footer_td.innerHTML = "&nbsp;";
        footer_row.appendChild(footer_td);

        table.appendChild(footer_row);
        

}

window.setInterval(refresh, 5000);
