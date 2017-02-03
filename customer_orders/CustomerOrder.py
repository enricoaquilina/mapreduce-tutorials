from mrjob.job import MRJob
from mrjob.step import MRStep

class CustomerOrder(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.map_get_orders,
                   reducer=self.red_sum_orders),
            MRStep(mapper=self.map_make_orders_key,
                   reducer=self.red_output_sorted)
        ]

    def map_get_orders(self, key, line):
        (user_id, item, order_price) = line.split(',')
        yield int(user_id), float(order_price)

    def red_sum_orders(self, user, total_orders):
        yield user, sum(total_orders)

    def map_make_orders_key(self, user, total_orders):
        yield '%04.02f'%float(total_orders), user

    def red_output_sorted(self, total_order, users):
        for user in users:
            yield user, total_order

if __name__ == '__main__':
    CustomerOrder.run()
