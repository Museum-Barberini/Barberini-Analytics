-- Add attribute and view for customer characteristics (!269)

BEGIN;

    ALTER TABLE gomus_customer ADD COLUMN tourism_tags TEXT[];

    CREATE VIEW customer_characteristics AS (
        WITH 
            o_general AS (
                SELECT 
                    goc.customer_id AS customer_id,
                    count(goc.o_id) AS order_count,
                    sum(goc.quantity_sum) AS ordered_article_count, 
                    sum(goc.order_price) AS sum_order_price 
                FROM (
                    SELECT 
                        o.order_id AS o_id,
                        sum(oc.quantity) AS quantity_sum,
                        sum(oc.price) AS order_price,
                        o.customer_id AS customer_id 
                    FROM 
                        gomus_order AS o
                        JOIN gomus_order_contains AS oc USING (order_id)
                    GROUP BY 
                        o.order_id,
                        o.customer_id
                    ) AS goc
                GROUP BY 
                    goc.customer_id
            ),
            o_common AS (
                SELECT 
                    customer_id,
                    (array_agg(
                        ticket_name ORDER BY ticket_quantity DESC
                    ))[1] AS most_common_ticket
                FROM (
                    SELECT 
                        o.customer_id AS customer_id,
                        oc.ticket AS ticket_name,
                        sum(quantity) AS ticket_quantity
                    FROM
                        gomus_customer AS c
                        JOIN gomus_order AS o USING (customer_id)
                        JOIN gomus_order_contains AS oc USING (order_id)
                    GROUP BY 
                        o.customer_id,
                        oc.ticket
                    ) AS grouped_ticket
                GROUP BY 
                    customer_id
            ),
            b_general AS (
                SELECT 
                    c.customer_id AS customer_id,
                    count(b.booking_id) AS booking_count, 
                    sum(b.participants) AS sum_booked_participants
                FROM 
                    gomus_customer AS c
                    JOIN gomus_booking AS b USING (customer_id)
                GROUP BY 
                    c.customer_id
            ),
            b_common_name AS (
                SELECT 
                    customer_id,
                    (array_agg(
                        title ORDER BY title_count DESC
                    ))[1] AS most_common_booking
                FROM (
                    SELECT 
                        customer_id,
                        title,
                        count(*) AS title_count
                    FROM 
                        gomus_booking AS b 
                    GROUP BY 
                        customer_id,
                        title
                    ) AS grouped_booking
                GROUP BY customer_id
            ), 
            b_common_category AS (
                SELECT 
                    customer_id,
                    (array_agg(
                        category ORDER BY category_count DESC
                    ))[1] AS most_common_booking_category
                FROM (
                    SELECT
                        customer_id,
                        category,
                        count(*) AS category_count
                    FROM 
                        gomus_booking AS b 
                    GROUP BY 
                        customer_id,
                        category
                    ) AS grouped_booking
                GROUP BY customer_id
            ),
            e_general AS (
                SELECT 
                    c.customer_id AS customer_id,
                    count(e.event_id) AS event_count, 
                    sum(e.reservation_count) AS sum_event_reservations
                FROM
                    gomus_customer AS c
                    JOIN gomus_event AS e USING (customer_id)
                GROUP BY 
                    c.customer_id
            ),
            e_common_name AS (
                SELECT 
                    grouped_event.customer_id AS customer_id,
                    (array_agg(
                        b.title ORDER BY booking_reservations_count DESC
                    ))[1] AS most_common_event
                FROM (
                    SELECT 
                        customer_id,
                        booking_id,
                        count(*) AS booking_reservations_count
                    FROM 
                        gomus_event AS e
                    GROUP BY 
                        customer_id,
                        booking_id
                    ) AS grouped_event
                    JOIN gomus_booking AS b USING (booking_id)
                GROUP BY 
                    grouped_event.customer_id
            ),
            e_common_category AS (
                SELECT 
                    customer_id,
                    (array_agg(
                        category ORDER BY category_count DESC
                    ))[1] AS most_common_event_category
                FROM (
                    SELECT 
                        customer_id,
                        category,
                        count(*) AS category_count
                    FROM 
                        gomus_event AS e
                    GROUP BY 
                        customer_id,
                        category
                    ) AS grouped_event
                GROUP BY 
                    customer_id
            )
        SELECT 
            c.*,
            order_count,
            ordered_article_count,
            sum_order_price,
            booking_count,
            sum_booked_participants,
            event_count,
            sum_event_reservations,
            most_common_ticket,
            most_common_booking,
            most_common_event,
            most_common_booking_category,
            most_common_event_category
        FROM 
            gomus_customer AS c
            LEFT JOIN o_general USING (customer_id)
            LEFT JOIN o_common USING (customer_id)
            LEFT JOIN b_general USING (customer_id)
            LEFT JOIN b_common_name USING (customer_id)
            LEFT JOIN b_common_category USING (customer_id)
            LEFT JOIN e_general USING (customer_id)
            LEFT JOIN e_common_name USING (customer_id)
            LEFT JOIN e_common_category USING (customer_id)
    );

COMMIT;
