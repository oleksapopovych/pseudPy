import pseudPy.Pseudonymization as pseudPy
from presidio_evaluator.data_generator import PresidioDataGenerator


def run():
    pass
    # TODO : Presidio - fake data generation

    sentence_templates = [
        "My name is {{name}}",
        "Please send it to {{address}}",
        "I just moved to {{city}} from {{country}}",
    ]

    data_generator = PresidioDataGenerator()
    fake_records = data_generator.generate_fake_data(
        templates=sentence_templates, n_samples=10
    )

    fake_records = list(fake_records)

    # Print the spans of the first sample
    #for i in range(len(fake_records)):
    #    print(fake_records[i].fake)
    #print(fake_records[0].spans)

    for i in range(len(fake_records)):
        print(fake_records[i].fake)
        pseudo = pseudPy.Pseudonymization(
            map_method='faker',
            text=fake_records[i].fake,
            pos_type=['Names', 'Locations']
        )
        print(pseudo.nlp_pseudonym())


if __name__ == '__main__':
    run()
