-- Add attribute and view for customer characteristics (!269)

BEGIN;

    ALTER TABLE gomus_customer ADD COLUMN tourism_tags TEXT[];

    CREATE VIEW customer_characteristics as (
        with 
            o_general as (
                select 
                    goc.customer_id as customer_id,
                    count(goc.o_id) as order_count,
                    sum(goc.quantity_sum) as ordered_article_count, 
                    sum(goc.order_price) as sum_order_price 
                from (
                    select 
                        o.order_id as o_id,
                        sum(oc.quantity) as quantity_sum,
                        sum(oc.price) as order_price,
                        o.customer_id as customer_id 
                    from 
                        gomus_order as o,
                        gomus_order_contains as oc 
                    where 
                        o.order_id = oc.order_id
                    group by 
                        o.order_id,
                        o.customer_id
                    ) as goc
                group by 
                    goc.customer_id
            ),
            o_common as (
                select 
                    customer_id,
                    (array_agg(
                        ticket_name ORDER BY ticket_quantity DESC
                    ))[1] as most_common_ticket
                from (
                    select 
                        o.customer_id as customer_id,
                        oc.ticket as ticket_name,
                        sum(quantity) as ticket_quantity
                    from 
                        gomus_customer as c,
                        gomus_order as o,
                        gomus_order_contains as oc 
                    where 
                        o.order_id=oc.order_id and
                        c.customer_id=o.customer_id 
                    group by 
                        o.customer_id,
                        oc.ticket
                    ) as grouped_ticket
                group by 
                    customer_id
            ),
            b_general as (
                select 
                    c.customer_id as customer_id,
                    count(b.booking_id) as booking_count, 
                    sum(b.participants) as sum_booked_participants
                from 
                    gomus_customer as c,
                    gomus_booking as b
                where 
                    c.customer_id=b.customer_id
                group by 
                    c.customer_id
            ),
            b_common_name as (
                select 
                    customer_id,
                    (array_agg(
                        title ORDER BY title_count DESC
                    ))[1] as most_common_booking
                from (
                    select 
                        customer_id,
                        title,
                        count(*) as title_count
                    from 
                        gomus_booking as b 
                    group by 
                        customer_id,
                        title
                    ) as grouped_booking
                group by customer_id
            ), 
            b_common_category as (
                select 
                    customer_id,
                    (array_agg(
                        category ORDER BY category_count DESC
                    ))[1] as most_common_booking_category
                from (
                    select
                        customer_id,
                        category,
                        count(*) as category_count
                    from 
                        gomus_booking as b 
                    group by 
                        customer_id,
                        category
                    ) as grouped_booking
                group by customer_id
            ),
            e_general as (
                select 
                    c.customer_id as customer_id,
                    count(e.event_id) as event_count, 
                    sum(e.reservation_count) as sum_event_reservations
                from 
                    gomus_customer as c,
                    gomus_event as e
                where 
                    c.customer_id=e.customer_id
                group by 
                    c.customer_id
            ),
            e_common_name as (
                select 
                    grouped_event.customer_id as customer_id,
                    (array_agg(
                        b.title ORDER BY booking_reservations_count DESC
                    ))[1] as most_common_event
                from (
                    select 
                        customer_id,
                        booking_id,
                        count(*) as booking_reservations_count
                    from 
                        gomus_event as e
                    group by 
                        customer_id,
                        booking_id
                    ) as grouped_event, 
                    gomus_booking as b
                where
                    grouped_event.booking_id=b.booking_id
                group by 
                    grouped_event.customer_id
            ),
            e_common_category as (
                select 
                    customer_id as customer_id,
                    (array_agg(
                        category ORDER BY category_count DESC
                    ))[1] as most_common_event_category
                from (
                    select 
                        customer_id,
                        category,
                        count(*) as category_count
                    from 
                        gomus_event as e
                    group by 
                        customer_id,
                        category
                    ) as grouped_event
                group by 
                    customer_id
            )
        select 
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
        from 
            gomus_customer as c
            left join o_general 
                on c.customer_id=o_general.customer_id
            left join o_common 
                on c.customer_id=o_common.customer_id
            left join b_general 
                on c.customer_id=b_general.customer_id
            left join b_common_name
                on c.customer_id=b_common_name.customer_id
            left join b_common_category
                on c.customer_id=b_common_category.customer_id
            left join e_general
                on c.customer_id=e_general.customer_id
            left join e_common_name
                on c.customer_id=e_common_name.customer_id
            left join e_common_category
                on c.customer_id=e_common_category.customer_id
    );

COMMIT;
