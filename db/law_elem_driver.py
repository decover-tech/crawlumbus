from db.law_elem import LawElemModel


class LawElemDriver:
    @staticmethod
    def add_law(db, law_name, jurisdiction, category, sub_category, url, file_name, title):
        new_law = LawElemModel(law_name, jurisdiction, category, sub_category, url, file_name, title)
        db.session.add(new_law)
        db.session.commit()

    @staticmethod
    def get_law(db, law_name):
        law = db.session.query(LawElemModel).filter_by(law_name=law_name).first()
        return law

    @staticmethod
    def update_law(db, law_name, **kwargs):
        law = db.session.query(LawElemModel).filter_by(law_name=law_name).first()
        for key, value in kwargs.items():
            setattr(law, key, value)
        db.session.commit()
