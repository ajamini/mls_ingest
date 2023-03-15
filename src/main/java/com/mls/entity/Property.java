package com.mls.entity;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;

public class Property {
    private final String url = "jdbc:postgresql://app-b3462b97-227b-4eeb-9bbd-aaba7213bc35-do-user-6916594-0.b.db.ondigitalocean.com:25060/realtor-app?ssl=true&sslmode=require";
    private final String user = "cao_scanner";
    private final String password = "AVNS_U560oG6AUrJz5Q8Bi5d";
    private static Connection connection;

    public Property() throws SQLException {
        if (Property.connection == null) {
            Property.connection = DriverManager.getConnection(url, user, password);
        }
    }
   
/*
create table mls_listing_photo (
    id serial PRIMARY KEY,
    listing_id integer not null,
    storage_url CHARACTER VARYING(250) not null,
    filename CHARACTER VARYING(250) not null,
    description varchar,
    FOREIGN KEY (listing_id) REFERENCES mls_listing (id)
);

create table mls_listing(
    id serial PRIMARY KEY,
    mls_number CHARACTER VARYING(50),
    status CHARACTER VARYING(50),
    address_street CHARACTER VARYING(100),
    address_abr CHARACTER VARYING(50),
    address_dir CHARACTER VARYING(50),
    address_unit CHARACTER VARYING(50),
    address_city CHARACTER VARYING(50),
    address_postal CHARACTER VARYING(10),
    address_province CHARACTER VARYING(50),
    address_full CHARACTER VARYING(250),
    community CHARACTER VARYING(100),
    community_code CHARACTER VARYING(50),
    municipal_code CHARACTER VARYING(50),
    municipal_district CHARACTER VARYING(50),
    municipal CHARACTER VARYING(100),
    address_cross_street CHARACTER VARYING(100),
    zoning CHARACTER VARYING(50),
    description VARCHAR,
    description_extras VARCHAR,
    possession_text VARCHAR,
    lease_terms VARCHAR,
    listing_price CHARACTER VARYING(20),
    square_footage CHARACTER VARYING(20),
    lot_depth_ft CHARACTER VARYING(10),
    lot_front_ft CHARACTER VARYING(10),
    taxes CHARACTER VARYING(20),
    bedrooms integer,    
    dens integer,
    bathroom integer,
    kitchens integer,
    kitchenates integer,
    rooms integer,
    room_extra integer,
    agent_name CHARACTER VARYING(250),
    brokerage_name CHARACTER VARYING(250)
);
 */

    public Integer createProperty(
            String mls_number, String status,
            String address_street, String address_abr, String address_dir, String address_unit, String address_city, String address_postal, String address_province, String address_full,
            String community, String community_code, String municipal_code, String municipal_district, String municipal,
            String address_cross_street, String zoning,
            String description, String description_extras, String possession_text, String lease_terms,
            String listing_price,
            String square_footage, String lot_depth_ft, String lot_front_ft, String taxes,
            Integer bedrooms, Integer dens, Integer bathroom, Integer kitchens, Integer kitchenate, Integer rooms, Integer room_extra,
            String agent_name, String brokerage_name
    ) {

        String insert = "Insert into mls_listing (mls_number, status, " +
                "address_street, address_abr, address_dir, address_unit, address_city, address_postal, address_province, address_full, " +
                "community, community_code, municipal_code, municipal_district, municipal, " +
                "address_cross_street, zoning, " +
                "description, description_extras, possession_text, lease_terms, listing_price, " +
                "square_footage, lot_depth_ft, lot_front_ft, taxes, " +
                "bedrooms, dens, bathroom, kitchens, kitchenates, rooms, room_extra, agent_name, brokerage_name) " +
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        Integer id = null;
        try (PreparedStatement preparedStatement = Property.connection.prepareStatement(insert, Statement.RETURN_GENERATED_KEYS)) {
            preparedStatement.setString(1, mls_number);
            preparedStatement.setString(2, status);

            preparedStatement.setString(3, address_street);
            preparedStatement.setString(4, address_abr);
            preparedStatement.setString(5, address_dir);
            preparedStatement.setString(6, address_unit);
            preparedStatement.setString(7, address_city);
            preparedStatement.setString(8, address_postal);
            preparedStatement.setString(9, address_province);
            preparedStatement.setString(10, address_full);

            preparedStatement.setString(11, community);
            preparedStatement.setString(12, community_code);
            preparedStatement.setString(13, municipal_code);
            preparedStatement.setString(14, municipal_district);
            preparedStatement.setString(15, municipal);

            preparedStatement.setString(16, address_cross_street);
            preparedStatement.setString(17, zoning);

            preparedStatement.setString(18, description);
            preparedStatement.setString(19, description_extras);
            preparedStatement.setString(20, possession_text);
            preparedStatement.setString(21, lease_terms);

            preparedStatement.setString(22, listing_price);

            preparedStatement.setString(23, square_footage);
            preparedStatement.setString(24, lot_depth_ft);
            preparedStatement.setString(25, lot_front_ft);
            preparedStatement.setString(26, taxes);

            preparedStatement.setInt(27, bedrooms);
            preparedStatement.setInt(28, dens);
            preparedStatement.setInt(29, bathroom);
            preparedStatement.setInt(30, kitchens);
            preparedStatement.setInt(31, kitchenate);
            preparedStatement.setInt(32, rooms);
            preparedStatement.setInt(33, room_extra);

            preparedStatement.setString(34, agent_name);
            preparedStatement.setString(35, brokerage_name);

            // Step 3: Execute the query or update query
            preparedStatement.execute();

            ResultSet rs = preparedStatement.getGeneratedKeys();
            if (rs.next()) {
                id = rs.getInt(1);
            }

        } catch (SQLException e) {
            // print SQL exception information
            System.out.println(e);
        }

        return id;
    }

    public void addImage(Integer id, String contentType, String description, String filename, String storageUrl) {
        String insert = "Insert into mls_listing_photo (listing_id, filename, storage_url)" +
                "VALUES (?,?,?)";

        try (PreparedStatement preparedStatement = this.connection.prepareStatement(insert, Statement.RETURN_GENERATED_KEYS)) {
            preparedStatement.setInt(1, id);
            preparedStatement.setString(2, filename);
            preparedStatement.setString(3, storageUrl);

            // Step 3: Execute the query or update query
            preparedStatement.execute();
        } catch (SQLException e) {
            // print SQL exception information
            System.out.println(e);
        }
    }
}
